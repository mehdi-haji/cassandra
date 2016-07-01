/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.async.paging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.mina.core.future.WriteFuture;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

/**
 * A collection of classes that send query results to a client asynchronously, that is as soon as a page
 * is available without the client requesting it, by using sessions
 * uniquely identified by a uuid set by the client. See CASSANDRA-11521 for more details.
 */
public class AsyncPagingService
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPagingService.class);
    private static final ConcurrentMap<UUID, PageBuilder> cancellableSessions = new ConcurrentHashMap<>();

    public static SelectStatement.Executor.PagingFactory pagingFactory(SelectStatement statement, QueryState state, QueryOptions options)
    throws RequestValidationException, RequestExecutionException
    {
        QueryOptions.AsyncPagingOptions pagingOptions = options.getAsyncPagingOptions();
        assert pagingOptions.asyncPagingRequested();

        checkFalse(cancellableSessions.containsKey(pagingOptions.uuid),
                   String.format("Invalid request, already executing a session with uuid %s", pagingOptions.uuid));

        logger.trace("Starting async paging session with id {} and paging state {}", pagingOptions.uuid, options.getPagingState());

        return new SelectStatement.Executor.PagingFactory()
        {
            public Selection.RowBuilder builder(QueryPager pager)
            {
                PageBuilder ret = new PageBuilder(statement,
                                                  state,
                                                  options,
                                                  pager,
                                                  new AsyncPagingImpl(state.getConnection(), options.getAsyncPagingOptions()));

                // add this session to the cancellable sessions, it will be removed by PageBuilder.complete()
                cancellableSessions.putIfAbsent(pagingOptions.uuid, ret);
                return ret;
            }

            public int pageSize()
            {
                // This is only used by non-optimized queries (SelectStatement.DistributedExecutor)
                // and doesn't necessarily correspond to the size of the pages sent to the client, which
                // may be different
                return pagingOptions.estimatedRows(statement.selection.estimatedRowSize());
            }
        };
    }

    /**
     * Cancel an ongoing async paging session.
     *
     * @param uuid - the unique identifier of the session to cancel.
     */
    public static void cancel(UUID uuid)
    {
        PageBuilder builder = cancellableSessions.get(uuid);
        if (builder == null || builder.isStopped())
        {
            logger.debug("Cannot cancel async paging session {}: not found or already stopped", uuid);
            return;
        }

        logger.debug("Cancelling async paging session {}", uuid);
        builder.cancel();
    }

    /**
     * Build pages of CQL rows by implementing the abstract methods of {@link Selection.RowBuilder}.
     * Unlike {@link Selection.ResultSetBuilder}, rows are written to a buffer as soon as they are available,
     * so as to avoid the cost of an additional List and more importantly of calling encodedSize for each one
     * of them. This is possible because we do not support sorting across partitions when paging is enabled
     * (CASSANDRA-6722) and we can enforce user limits on rows directly in this builder. The buffer the rows
     * are written to is managed differently depending on the paging unit. If the page size is in bytes, then
     * it's pretty straightforward: we write rows as long as there is space in the buffer and we send a page
     * and start a new one when we run out of space. If the pages size is in rows, we instead start with a
     * buffer of an estimated size and double it, up to a maximum, if we run out of space before we have
     * reached our target number of rows. The buffer is reused if the buffer size of the next page matches the
     * size of the previous page. If the previous page was bigger, we could reuse it but we risk that abnormally
     * large rows
     */
    private final static class PageBuilder extends Selection.RowBuilder
    {
        /**
         * The callback will actually implement the async paging concerns
         * by sending a page to the client, see {@link AsyncPagingImpl}.
         */
        interface Callback
        {
            public void onPage(Page page);
        }

        /**
         * A page: this wraps a buffer and keeps useful information
         * such as metadata, the number of rows and whether this is the last page.
         * It can then create a Frame using this information, at which point
         * it is free to reuse the buffer, if possible, or to release it.
         */
        static class Page
        {
            // The metadata attached to the response
            final ResultSet.ResultMetadata metadata;

            // the state is needed to fix the response parameters
            final QueryState state;

            // the paging options
            final QueryOptions.AsyncPagingOptions pagingOptions;

            // The protocol version
            final int version;

            // the buffer where the rows will be written
            ByteBuf buf;

            // the number of rows already written to the buffer
            int numRows;

            // the page sequential number
            int seqNo;

            Page(int bufferSize, ResultSet.ResultMetadata metadata, QueryState state, QueryOptions options, int seqNo)
            {
                this.metadata = metadata;
                this.state = state;
                this.pagingOptions = options.getAsyncPagingOptions();
                this.version = options.getProtocolVersion();

                this.buf = CBUtil.allocator.buffer(bufferSize);
                this.seqNo = seqNo;
            }

            /**
             * Calculates the header size and creates a frame. Copies the page header and rows into it.
             * We could try avoiding the copy of the row data using Netty CompositeByteBuf but on the
             * other hand if we copy then we can reuse the buffer and so it's probably not too bad.
             *
             * @return a newly created frame, the caller takes ownership of the frame body buffer.
             */
            Frame makeFrame()
            {
                ResultMessage response = makeResponse();
                int messageSize = ResultMessage.codec.encodedSize(response, version);
                Frame frame = Message.ProtocolEncoder.makeFrame(response, messageSize, version);
                ResultMessage.codec.encode(response, frame.body, version);

                // This code was useful for debugging, by decoding the rows immediately
//                try
//                {
//                    ResultMessage msg = ResultMessage.codec.decode(frame.body, version);
//                    frame.body.readerIndex(0);
//                    assert msg instanceof ResultMessage.Rows;
//                    ResultSet result = ((ResultMessage.Rows) msg).result;
//                    assert result.size() == numRows;
//                }
//                catch (Exception ex)
//                {
//                    logger.info("Failed to decode {} rows: {}, {}, {}",
//                                numRows, buf,messageSize, frame.body.capacity());
//                    ex.printStackTrace();
//                    throw ex;
//                }

                return frame;
            }

            /** This is a template response that will be used to create every page Frame */
            private ResultMessage makeResponse()
            {
                ResultMessage response = new ResultMessage.EncodedRows(metadata, numRows, buf);
                response.setStreamId(-1);
                response.setWarnings(ClientWarn.instance.getWarnings());
                if (state.getPreparedTracingSession() != null)
                    response.setTracingId(state.getPreparedTracingSession());
                return response;
            }

            /** Add a row to the buffer.
             *
             * If we've run out of space double the buffer size, up to MAX_PAGE_SIZE_BYTES. However make
             * sure that at a minimum we have enough space for adding the current row. */
            void addRow(List<ByteBuffer> row)
            {
                int prevWriteIndex = buf.writerIndex();
                boolean ret = ResultSet.codec.encodeRow(row, metadata, buf, true);
                if (ret)
                {
                    numRows++;
                    return;
                }

                buf.writerIndex(prevWriteIndex);
                int rowSize = ResultSet.codec.encodedRowSize(row, metadata);
                int bufferSize = Math.max(buf.readableBytes() + rowSize, Math.min(buf.capacity() * 2, maxPageSize()));
                logger.trace("Reallocating page buffer from {}/{} to {} for row size {} - {}",
                             buf.readableBytes(), buf.capacity(), bufferSize, rowSize, pagingOptions.uuid);

                ByteBuf old = buf;
                try
                {
                    buf = null;
                    buf = CBUtil.allocator.buffer(bufferSize);
                    buf.writeBytes(old);
                    ResultSet.codec.encodeRow(row, metadata, buf, false);
                    numRows++;
                }
                finally
                {
                    old.release();
                }
            }

            int size()
            {
                return buf.readableBytes();
            }

            boolean isEmpty()
            {
                return numRows == 0;
            }

            void reuse(int seqNo)
            {
                this.numRows = 0;
                this.seqNo = seqNo;
                this.buf.clear();
            }

            void release()
            {
                buf.release();
                buf = null;
            }

            boolean last()
            {
                Optional<AsyncPagingParams> asyncPagingParams = metadata.asyncPagingParams();
                return asyncPagingParams.isPresent() && asyncPagingParams.get().last;
            }

            /**
             * Return an average row size by calculating this page average and averaging it with the existing one.
             *
             * @param current - the current average row size in bytes
             * @return - the new average row size in bytes
             */
            int avgRowSize(int current)
            {
                if (buf == null || numRows == 0)
                    return current;

                int avg = buf.readableBytes() / numRows;
                return (avg + current) / 2;
            }

            @Override
            public String toString()
            {
                return String.format("[Page seqNo: %d, rows: %d, %s, %s]", seqNo, numRows, metadata.pagingState(), metadata.asyncPagingParams());
            }
        }

        /** The ResultSet metadata is needed as the header in the page */
        private final ResultSet.ResultMetadata resultMetaData;

        /** A template response for creating the page frame */
        private final QueryState state;

        /** The query options contain some parameters that we need */
        private final QueryOptions options;

        /** The callback to process completed pages, see {@link Callback} */
        private final Callback callback;

        /** The query pager responsible for the iteration, we actually just carry this for the callback */
        private final QueryPager pager;

        /** The paging options, including paging unit, size and max number of pages */
        private final QueryOptions.AsyncPagingOptions pagingOptions;

        /** The average row size, initially estimated by the selection and then refined each time a page is sent */
        private int avgRowSize;

        /** The current page being written to */
        private Page currentPage;

        /** Set to true when a cancel request has been received */
        private volatile boolean cancelRequested;

        /** Set to true when the session has been stopped, either because of limits or a cancel request. */
        private volatile boolean stopped;

        PageBuilder(SelectStatement statement,
                    QueryState state,
                    QueryOptions options,
                    QueryPager pager,
                    Callback callback)
        {
            super(options, statement.parameters.isJson, statement.selection);
            this.resultMetaData = selection.getResultMetadata(isJson).copy();
            this.state = state;
            this.options = options;
            this.pager = pager;
            this.pagingOptions = options.getAsyncPagingOptions();
            this.callback = callback;
            this.avgRowSize = selection.estimatedRowSize();

            allocatePage(1);
        }

        /** Request to stop an ongoing session. */
        public void cancel()
        {
            cancelRequested = true;
        }

        public boolean isStopped()
        {
            return stopped;
        }

        /** Allocate the initial page with the page size specified by the user and some safe margin added to it,
         * but making sure it is not too big by enforcing MAX_PAGE_SIZE_BYTES as an upper limit. Otherwise
         * reuse an existing page.
         *
         * @param seqNo - the sequence number for the new page
         */
        private void allocatePage(int seqNo)
        {
            if (currentPage == null)
            {
                int bufferSize = Math.min(maxPageSize(), options.getAsyncPagingOptions().bufferSize(avgRowSize) + safePageMargin());
                logger.trace("Allocating page with buffer size {}, avg row size {} for {}", bufferSize, avgRowSize, pagingOptions.uuid);
                currentPage = new Page(bufferSize, resultMetaData, state, options, seqNo);
            }
            else
            {
                logger.trace("Reusing page with buffer size {}, avg row size {} for {}", currentPage.buf.capacity(), avgRowSize, pagingOptions.uuid);
                currentPage.reuse(seqNo);
            }
        }

        /**
         * User page sizes bigger than this value will be ignored and this value will be used instead.
         *
         * @return - the max page size in bytes
         */
        static int maxPageSize()
        {
            return DatabaseDescriptor.getNativeTransportMaxFrameSize() / 2;
        }

        /**
         * Return the safe page margin.
         *
         * @return a number that we add to the page buffer size to reduce the probability of having to reallocate
         * to fit all page rows. Also, if a page is close to MAX_PAGE_SIZE_BYTES by this margin, we force a page
         * to be sent regardless of page limits.
         */
        private int safePageMargin()
        {
            return 2 * avgRowSize;
        }

        /**
         * Send the page to the callback.
         *
         * It's OK to send empty pages if they are the last page, because the client needs
         * to know it has received the last page. Also update the average row size.
         */
        private void processPage(boolean last)
        {
            assert !currentPage.isEmpty() || last;

            pager.saveState();
            currentPage.metadata.setHasMorePages(pager.isExhausted() ? null : pager.state());
            currentPage.metadata.setAsyncPagingParams(new AsyncPagingParams(pagingOptions.uuid, currentPage.seqNo, last));
            avgRowSize = currentPage.avgRowSize(avgRowSize);

            callback.onPage(currentPage);
        }

        /**
         * A row is available: add the row to the current page and see if we must send
         * the current page. Allocate a new page if the current page was sent, stop
         * if we need to do so.
         *
         * @param row - the completed row
         * @return - true if we should continue processing more rows, false otherwise.
         */
        public boolean onRowCompleted(List<ByteBuffer> row)
        {
            if (currentPage == null)
            {
                assert stopped;
                return false;
            }

            currentPage.addRow(row);

            if (cancelRequested)
                stop();

            boolean mustSendPage = pagingOptions.completed(currentPage.numRows, currentPage.size(), avgRowSize) || pageIsCloseToMax();
            if (mustSendPage)
            {
                boolean isLastPage = isLastPage(currentPage.seqNo);
                processPage(isLastPage);
                if (!isLastPage)
                {
                    allocatePage(currentPage.seqNo + 1);
                }
                else
                {
                    stop();
                    currentPage.release();
                    currentPage = null;
                }
            }

            return !stopped;
        }

        private boolean isLastPage(int pageNo)
        {
            return pagingOptions.maxPages > 0 && pageNo >= pagingOptions.maxPages;
        }

        private boolean pageIsCloseToMax()
        {
            return currentPage != null && (maxPageSize() - currentPage.size()) < safePageMargin();
        }

        private void stop()
        {
            if (!stopped)
            {
                logger.debug("Stopping {} early", pagingOptions.uuid);
                pager.stop();
                stopped = true;
            }
        }

        public void setHasMorePages(PagingState pagingState)
        {
            if (currentPage != null)
                currentPage.metadata.setHasMorePages(pagingState);
        }

        public boolean resultIsEmpty()
        {
            return currentPage == null || currentPage.isEmpty();
        }

        @Override
        public void complete()
        {
            super.complete();

            if (currentPage != null)
            {
                processPage(true);
                currentPage.release();
            }

            // once completed, no need to allow cancelling this session
            cancellableSessions.remove(pagingOptions.uuid, this);
        }
    }

    /**
     * The class responsible for sending pages to the user asynchronously.
     * It does so via {@link AsyncPageWriter}.
     */
    private final static class AsyncPagingImpl implements PageBuilder.Callback
    {
        /** When offering a page to the client, wait for at most this time */
        private final static int SEND_TIMEOUT_MSECS = 100;
        /** Number of attempts when offering a page to the client before we give up */
        private final static int FAILED_SEND_NUM_ATTEMPTS = 100;

        private final Connection connection;
        private final QueryOptions.AsyncPagingOptions options;
        private final AsyncPageWriter pageWriter;

        AsyncPagingImpl(Connection connection, QueryOptions.AsyncPagingOptions options)
        {
            this.connection = connection;
            this.options = options;
            this.pageWriter =  new AsyncPageWriter(connection, options.maxPagesPerSecond);
        }

        /**
         * A page is reqdy to be sent, let's save paging and streaming state and send this
         * page to the {@link AsyncPageWriter} queue.
         *
         * @param page - the page to be sent
         */
        public void onPage(PageBuilder.Page page)
        {
            logger.trace("Processing {}", page);

            int i = 0;
            boolean sent = false;
            Frame frame = page.makeFrame();
            while (!sent && i++ < FAILED_SEND_NUM_ATTEMPTS)
                sent = pageWriter.sendPage(frame, !page.last(), SEND_TIMEOUT_MSECS);

            if (!sent)
            {
                frame.release();
                throw new RuntimeException(String.format("Timed-out sending page no. %d of session %s, given up",
                                                         page.seqNo,
                                                         options.uuid));
            }

            if (page.seqNo == 1)
                writeToChannel(pageWriter);
        }

        /**
         * Write the page writer (a chunked input implementation) to the channel so that Netty's chunked write
         * handler (already added to the channel pipe) will try to send pages to the client when it can accept
         * them, see {@link io.netty.handler.stream.ChunkedInput} and {@link io.netty.handler.stream.ChunkedWriteHandler}.
         */
        private void writeToChannel(Object obj)
        {
            Channel channel = connection.channel();
            ChannelPromise promise = channel.newPromise();
            promise.addListener(fut -> {
                if (!fut.isSuccess())
                {
                    Throwable t = fut.cause();
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.error("Failed writing {} for {}", obj, options.uuid, t);
                }
            });

            channel.writeAndFlush(obj, promise);
        }

    }
}
