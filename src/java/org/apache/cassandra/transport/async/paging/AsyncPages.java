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
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class AsyncPages implements ChunkedInput<ResultMessage.Rows>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPages.class);
    private static final int MAX_CONCURRENT_NUM_PAGES = 3; // max number of pending pages before we block

    private final ArrayBlockingQueue<ResultMessage.Rows> pages;
    private final ChunkedWriteHandler handler;
    private final AtomicBoolean suspended = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicInteger numSent = new AtomicInteger(0);

    public AsyncPages(Connection connection)
    {
        this.pages = new ArrayBlockingQueue<>(MAX_CONCURRENT_NUM_PAGES);
        this.handler = (ChunkedWriteHandler)connection.channel().pipeline().get("chunkedWriter");
    }

    /** Adds a page to the queue so that it can be sent later on when the channel is available.
     * This method will block for up to timeoutMillis if the queue if full.
     */
    public boolean sendPage(ResultSet resultSet, long timeoutMillis)
    {
        try
        {
            ResultMessage.Rows response = new ResultMessage.Rows(resultSet);
            response.setStreamId(-1);

            boolean ret = pages.offer(response, timeoutMillis, TimeUnit.MILLISECONDS);
            if (ret)
            {
                maybeResumeTransfer();
                if (!resultSet.metadata.hasMorePages())
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
        logger.info("Closed: {}, {}, {}", pages.size(), completed, numSent.get());
        pages.clear();
    }

    public ResultMessage.Rows readChunk(ChannelHandlerContext channelHandlerContext) throws Exception
    {
        ResultMessage.Rows response = pages.poll();
        if (response == null)
            suspendTransfer();
        else
            numSent.incrementAndGet();
        return response;
    }

    private void suspendTransfer()
    {
        while (!suspended.get())
        {
            if (suspended.compareAndSet(false, true))
                break;
        }
    }

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
