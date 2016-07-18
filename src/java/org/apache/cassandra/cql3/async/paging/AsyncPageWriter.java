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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.math.RandomUtils;
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
class AsyncPageWriter implements ChunkedInput<Frame>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPageWriter.class);
    private static final int MAX_CONCURRENT_NUM_PAGES = 5; // max number of pending pages before we block

    private final ArrayBlockingQueue<Frame> pages;
    private final ChunkedWriteHandler handler;
    private final RateLimiter limiter;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger numSent = new AtomicInteger(0);

    AsyncPageWriter(Connection connection, int maxPagesPerSecond)
    {
        this.pages = new ArrayBlockingQueue<>(MAX_CONCURRENT_NUM_PAGES);
        this.handler = (ChunkedWriteHandler)connection.channel().pipeline().get(Server.CHUNKED_WRITER);
        this.limiter = RateLimiter.create(maxPagesPerSecond > 0 ? maxPagesPerSecond : Double.MAX_VALUE);
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

//            if (pages.size() > 0)
//                handler.resumeTransfer();

            if (ret)
            {
                handler.resumeTransfer();
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
            logger.trace("Closing chunked input, pending pages: {}, completed: {}, num sent: {}", pages.size(), completed, numSent.get());
            pages.clear();
        }
    }

    /**
     * Removes a page from the queue and returns it to the caller, the chunked writer, which will write
     * it to the client. We can return null to indicate there is nothing to read at the moment, but if
     * we do this then we must call {@link this#handler#resumeTransfer()} later on when we do have something to read.
     * If we cannot acquire a permit from the reader, we try again after a small and random amount of time.
     *
     * @return a page to write to the client, null if no page is available.
     */
    public Frame readChunk(ChannelHandlerContext channelHandlerContext) throws Exception
    {
        if (pages.peek() == null)
            return null;

        if (!limiter.tryAcquire())
        {
            // if we couldn't get a permit, rather than blocking a Netty thread, try again after a small random amount of time
            channelHandlerContext.executor().schedule(handler::resumeTransfer, RandomUtils.nextInt(100), TimeUnit.MICROSECONDS);
            return null;
        }

        Frame response = pages.poll();
        if (response != null)
            numSent.incrementAndGet();

        return response;
    }
}
