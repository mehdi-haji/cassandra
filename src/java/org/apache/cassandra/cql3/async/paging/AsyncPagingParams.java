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

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;

/**
 * The current status of an asynchronous paging result.
 */
public class AsyncPagingParams
{
    /** The unique identifier of the session this message is part of */
    public final UUID uuid;

    /** A unique sequential number to identify this message */
    public final int seqNo;

    /** True when this is the last message for this session */
    public final boolean last;

    public static final CBCodec<AsyncPagingParams> codec = new Codec();


    public AsyncPagingParams(UUID uuid, int seqNo, boolean last)
    {
        this.uuid = uuid;
        this.seqNo = seqNo;
        this.last = last;
    }

    @Override
    public String toString()
    {
        return String.format("[Async Paging Params %s - no. %d%s]", uuid, seqNo, last ? " final" : "");
    }

    private static class Codec implements CBCodec<AsyncPagingParams>
    {
        public AsyncPagingParams decode(ByteBuf body, int version)
        {
           return new AsyncPagingParams(CBUtil.readUUID(body), body.readInt(), body.readBoolean());
        }

        public void encode(AsyncPagingParams asyncPagingParams, ByteBuf dest, int version)
        {
            CBUtil.writeUUID(asyncPagingParams.uuid, dest);
            dest.writeInt(asyncPagingParams.seqNo);
            dest.writeBoolean(asyncPagingParams.last);
        }

        public int encodedSize(AsyncPagingParams asyncPagingParams, int version)
        {
            return CBUtil.sizeOfUUID(asyncPagingParams.uuid) + 5;
        }
    }
}
