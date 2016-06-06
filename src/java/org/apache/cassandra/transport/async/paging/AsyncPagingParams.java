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

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;

/**
 * The current status of an asynchronous paging result.
 */
public class AsyncPagingParams
{
    public final UUID uuid;
    public final int seqNo;

    public static final CBCodec<AsyncPagingParams> codec = new Codec();


    public AsyncPagingParams(UUID uuid, int seqNo)
    {
        this.uuid = uuid;
        this.seqNo = seqNo;
    }

    @Override
    public String toString()
    {
        return String.format("%s - no. %d", uuid, seqNo);
    }

    private static class Codec implements CBCodec<AsyncPagingParams>
    {
        public AsyncPagingParams decode(ByteBuf body, int version)
        {
           return new AsyncPagingParams(CBUtil.readUUID(body), body.readInt());
        }

        public void encode(AsyncPagingParams asyncPagingParams, ByteBuf dest, int version)
        {
            CBUtil.writeUUID(asyncPagingParams.uuid, dest);
            dest.writeInt(asyncPagingParams.seqNo);
        }

        public int encodedSize(AsyncPagingParams asyncPagingParams, int version)
        {
            return CBUtil.sizeOfUUID(asyncPagingParams.uuid) + 4;
        }
    }
}
