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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A netty chunked input composed of a queue of pages. {@link AsyncPagingService} puts pages into this queue
 * whilst a Netty ChunkWriteHandler called {@link Server#CHUNKED_WRITER} reads pages from this queue and writes
 * them to the client, when it is ready to receive them, that is when the channel is writable.
 */
class AsyncPages implements ChunkedInput<Frame>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPages.class);
    private static final int MAX_CONCURRENT_NUM_PAGES = 5; // max number of pending pages before we block

    private final ArrayBlockingQueue<Frame> pages;
    private final ChunkedWriteHandler handler;
    private final AtomicBoolean suspended = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger numSent = new AtomicInteger(0);

    AsyncPages(Connection connection)
    {
        this.pages = new ArrayBlockingQueue<>(MAX_CONCURRENT_NUM_PAGES);
        this.handler = (ChunkedWriteHandler)connection.channel().pipeline().get(Server.CHUNKED_WRITER);
    }

    /**
     * Adds a page to the queue so that it can be sent later on when the channel is available.
     * This method will block for up to timeoutMillis if the queue if full.
     */
    boolean sendPage(Frame frame, boolean hasMorePages, long timeoutMillis)
    {
        if (closed.get())
            throw new RuntimeException("Chunked input was closed");

        try
        {
            boolean ret = pages.offer(frame, timeoutMillis, TimeUnit.MILLISECONDS);
            if (ret)
            {
                maybeResumeTransfer();
                if (!hasMorePages)
                {
                    if (!completed.compareAndSet(false, true))
                        assert false : "Unexpected completed status";
                }
            }
            return ret;
        }
        catch (InterruptedException e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("Interrupted whilst sending page", e);
            return false;
        }
    }

    public boolean isEndOfInput() throws Exception
    {
        return completed.get() && pages.isEmpty();
    }

    public void close() throws Exception
    {
        if (closed.compareAndSet(false, true))
        {
            logger.info("Closing chunked input, pending pages: {}, completed: {}, num sent: {}", pages.size(), completed, numSent.get());
            pages.clear();
        }
    }

    /**
     * Removes a page from the queue and returns it to the caller, the chunked writer, which will write
     * it to the client. We can return null to indicate there is nothing to read at the moment, but if
     * we do this then we must call {@link this#maybeResumeTransfer()} later on when we do have something to read,
     * for this reason we must set {@link this#suspended} to true if we return null.
     *
     * @return a page to write to the client, null if no page is available.
     */
    public Frame readChunk(ChannelHandlerContext channelHandlerContext) throws Exception
    {
        Frame response = pages.poll();
        while (response == null)
        {
            if (isSuspended())
                break;

            response = pages.poll();
        }
        if (response != null)
            numSent.incrementAndGet();

        return response;
    }

    /**
     * Try to set {@link this#suspended} to true.
     *
     * @return true if suspended is true, false otherwise
     */
    private boolean isSuspended()
    {
        suspended.compareAndSet(false, true);
        return suspended.get();
    }

    /**
     * Try to set {@link this#suspended} to false.
     *
     * If we succeed then call handler.resumeTransfer().
     */
    private void maybeResumeTransfer()
    {
        while (suspended.get())
        {
            if (suspended.compareAndSet(true, false))
            {
                handler.resumeTransfer();
                break;
            }
        }
    }
}
