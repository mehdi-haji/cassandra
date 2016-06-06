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

package org.apache.cassandra.transport.async.paging;

import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Asynchronously send query results to a client using sessions uniquely identified by a uuid set by the client.
 */
public class AsyncPagingService
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPagingService.class);

    public static ResultMessage execute(SelectStatement statement, QueryState state, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        QueryOptions.AsyncPagingOptions asyncPagingOptions = options.getAsyncPagingOptions();
        assert asyncPagingOptions.asyncPagingRequested();

        logger.info("Starting async paging session with id {}", asyncPagingOptions.uuid);
        StageManager.getStage(Stage.READ).execute(new ReadRunnable(statement, state, options));
        return new ResultMessage.Void();
    }

    /**
     * Build a ResultSet by implementing the abstract methods of {@link Selection.RowBuilder}.
     */
    final static class PageBuilder extends Selection.RowBuilder implements AutoCloseable
    {
        interface Callback
        {
            public void onPage(Page page);
        }

        /**
         * A page: this wraps a frame and keeps useful information
         * such as the number of rows and the initial position
         */
        static class Page
        {
            // the size of the actual page (for writing rows only)
            final int pageSize;

            // the state is needed to fix the response parameters
            final QueryState state;

            // The metadata attached to the response
            final ResultSet.ResultMetadata metadata;

            // The protocol version
            final int version;

            // the buffer where the rows are written
            ByteBuf buf;

            // the number of rows already written to the buffer
            int numRows;

            Page(int pageSize, ResultSet.ResultMetadata metadata, QueryState state, int version)
            {
                this.pageSize = pageSize;
                this.metadata = metadata;
                this.state = state;
                this.version = version;

                this.buf = CBUtil.allocator.buffer(pageSize);
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
                //logger.info("Message size {} for resp with buf {}", messageSize, ((ResultMessage.EncodedRows)response).buff);
                Frame frame = Message.ProtocolEncoder.makeFrame(response, messageSize, version);
                ResultMessage.codec.encode(response, frame.body, version);

//                try
//                {
//                    ResultMessage msg = ResultMessage.codec.decode(frame.body, version);
//                    frame.body.readerIndex(0);
//                    assert msg instanceof ResultMessage.Rows;
//                    assert ((ResultMessage.Rows) msg).result.size() == numRows;
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

            /** This is a template response that will be use to create every page Frame */
            private ResultMessage makeResponse()
            {
                ResultMessage response = new ResultMessage.EncodedRows(metadata, numRows, buf);
                response.setStreamId(-1);
                response.setWarnings(ClientWarn.instance.getWarnings());
                if (state.getPreparedTracingSession() != null)
                    response.setTracingId(state.getPreparedTracingSession());
                return response;
            }

            boolean addRow(List<ByteBuffer> row)
            {
                int current = buf.writerIndex();
                boolean ret = ResultSet.codec.encodeRow(row, metadata, buf, true);
                if (ret)
                    numRows++;
                else
                    buf.writerIndex(current);
                return ret;
            }

            boolean isEmpty()
            {
                return numRows == 0;
            }

            void reuse()
            {
                numRows = 0;
                buf.clear();
            }

            void release()
            {
                buf.release();
                buf = null;
            }
        }

        /**
         * User page sizes bigger than this value will be ignored and this value will be used instead.
         * We use the max frame size minus 1k, which is reserved for the frame header, frame objects controlled by
         * the flags such us tracing and warnings, and page header (metadata and num rows). Some of these have
         * variable size so we can only approximate how much space they wil take, 1k should be more than sufficient,
         * and also at the moment we only enforce the maximum frame length when receiving frames, not when sending them.
         */
        private final static int MAX_PAGE_SIZE_BYTES = DatabaseDescriptor.getNativeTransportMaxFrameSize() - 1024;

        /** The ResultSet metadata is needed as the header in the page */
        private final ResultSet.ResultMetadata resultMetaData;

        /** A template response for creating the page frame */
        private final QueryState state;

        /** The query options contain some parameters that we need */
        private final QueryOptions options;

        private final QueryPager pager;

        private final Callback callback;

        /** The current page being written to */
        private Page currentPage;

        /** If the page specified by a user is so small that we can't fit even one row, we set this to the size of a row
         * so that we always allocate enough space to write at least one row.
         */
        private int minPageSize;

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
            this.callback = callback;

            allocatePage();
        }

        /** Allocate a new page: get the page size specified by the user in the paging options but make
         * sure it is not too big or too small, then add the header size and create a frame for the total
         * message size (header and page size) and using the response template to se the correct flags.
         *
         */
        private void allocatePage()
        {
            int pageSize = Math.max(minPageSize, Math.min(MAX_PAGE_SIZE_BYTES, options.getAsyncPagingOptions().pageSize));
            if (currentPage != null && currentPage.buf.capacity() == pageSize)
            {
                currentPage.reuse();
            }
            else
            {
                if (currentPage != null)
                {
                    currentPage.release();
                    currentPage = null;
                }
                logger.info("Allocating page with size {}", pageSize);
                currentPage = new Page(pageSize, resultMetaData, state, options.getProtocolVersion());
            }
        }

        /**
         * Process the current page and create a new one.
         */
        private void nextPage()
        {
            processPage(false);
            allocatePage();
        }

        /**
         * Write the header to the current page and send it to the callback unless it is empty.
         */
        private void processPage(boolean closing)
        {
            if (currentPage == null)
                return;

            if (!currentPage.isEmpty() || closing)
            {
                pager.saveState();

                if (!closing)
                    currentPage.metadata.setHasMorePages(pager.state());
                else
                    currentPage.metadata.setHasMorePages(null);

                callback.onPage(currentPage);
            }
        }

        public void onRowCompleted(List<ByteBuffer> row)
        {
            if (!currentPage.addRow(row))
            {
                nextPage();
                if (!currentPage.addRow(row))
                {   // the user page must be too small even for one row,
                    // try again with a minimum page size
                    minPageSize = ResultSet.codec.encodedRowSize(row, resultMetaData);
                    allocatePage();
                    if (!currentPage.addRow(row))
                    {
                        throw new RuntimeException(String.format("Failed to write row to fixed-size page for session %s",
                                                                 options.getAsyncPagingOptions().uuid));
                    }
                }
            }
        }

        public boolean resultIsEmpty()
        {
            return currentPage.isEmpty();
        }

        public void close() throws Exception
        {
            completeCurrentRow();
            processPage(true);
        }
    }

    private final static class ReadRunnable implements Runnable, PageBuilder.Callback
    {
        /** When offering a page to the client, wait for at most this time */
        private final static int SEND_TIMEOUT_MSECS = 100;
        /** Number of attempts when offering a page to the client before we give up */
        private final static int FAILED_SEND_NUM_ATTEMPTS = 50;

        private final SelectStatement statement;
        private final QueryState state;
        private final QueryOptions queryOptions;
        private final QueryOptions.AsyncPagingOptions pagingOptions;

        private final CFMetaData cfm;
        private final int nowInSec;
        private final ReadQuery query;
        private final AsyncPages pages;

        private int seqNo = 1;

        ReadRunnable(SelectStatement statement,
                     QueryState state,
                     QueryOptions queryOptions)
        {
            this.statement = statement;
            this.state = state;
            this.queryOptions = queryOptions;
            this.pagingOptions = queryOptions.getAsyncPagingOptions();
            this.cfm = statement.cfm;
            this.nowInSec = FBUtilities.nowInSeconds();
            this.query = statement.getQuery(queryOptions, nowInSec);
            this.pages = new AsyncPages(state.getConnection());
        }

        public void run() throws RequestExecutionException, RequestValidationException
        {
            try (ReadExecutionController executionController = query.executionController())
            {
                // we could avoid using the pager, it just creates the correct read command for us, by
                // taking into account the old paging state, and creates a new paging state when we need
                // it, we could do this without a pager now that we use fixed size pages
                QueryPager pager = query.getPager(queryOptions.getPagingState(), queryOptions.getProtocolVersion());
                try (PartitionIterator partitions = pager.fetchMultiplePagesInternal(Integer.MAX_VALUE, executionController);
                     PageBuilder builder = new PageBuilder(statement, state, queryOptions, pager, this))
                {
                    while (partitions.hasNext())
                    {
                        try (BaseRowIterator data = partitions.next())
                        {
                            processPartition(data, builder);
                        }
                    }
                }

                logger.info("Finished iterating {}", pagingOptions.uuid);
            }
            catch (Throwable ex)
            {
                JVMStabilityInspector.inspectCommitLogThrowable(ex);
                logger.error("Failed to stream pages for query {}", pagingOptions.uuid, ex);
            }
        }

        void processPartition(BaseRowIterator partition, PageBuilder builder) throws InvalidRequestException
        {
            int protocolVersion = queryOptions.getProtocolVersion();

            ByteBuffer[] keyComponents = SelectStatement.getComponents(cfm, partition.partitionKey());

            Row staticRow = partition.staticRow();
            // If there is no rows, and there's no restriction on clustering/regular columns,
            // then provided the select was a full partition selection (either by partition key and/or by static column),
            // we want to include static columns and we're done.
            if (!partition.hasNext())
            {
                if (!staticRow.isEmpty() && (!statement.restrictions.hasClusteringColumnsRestriction() || cfm.isStaticCompactTable()))
                    builder.addStaticRow(staticRow, keyComponents, statement.selection.getColumns(), nowInSec, protocolVersion);

                return;
            }

            while (partition.hasNext())
                builder.addRow((Row) partition.next(), staticRow, keyComponents, statement.selection.getColumns(), nowInSec, protocolVersion);
        }

        public void onPage(PageBuilder.Page page)
        {
            logger.debug("Processing page with {} rows and seq no. {} for {}", page.numRows, seqNo, pagingOptions.uuid);
            page.metadata.setStreamingState(new AsyncPagingParams(pagingOptions.uuid, seqNo));

            int i = 0;
            boolean sent = false;
            Frame frame = page.makeFrame();
            while (!sent && i++ < FAILED_SEND_NUM_ATTEMPTS)
                sent = pages.sendPage(frame, page.metadata.hasMorePages(), SEND_TIMEOUT_MSECS);

            if (!sent)
                throw new RuntimeException(String.format("Timed-out sending page no. %d of session %s, given up",
                                                         seqNo,
                                                         pagingOptions.uuid));

            if (seqNo == 1)
            {
                logger.info("Writing chunked input");
                Channel channel = state.getConnection().channel();
                ChannelPromise promise = channel.newPromise();
                promise.addListener(fut -> {
                    if (fut.isSuccess())
                    {
                        logger.info("Finished writing response for {}", pagingOptions.uuid);
                    }
                    else
                    {
                        Throwable t = fut.cause();
                        JVMStabilityInspector.inspectThrowable(t);
                        logger.error("Failed writing response for {}", pagingOptions.uuid, t);
                    }
                });
                channel.writeAndFlush(pages, promise);
            }

            seqNo++;
        }
    }
}
