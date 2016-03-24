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

package org.apache.cassandra.transport.messages;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Cancel a long running operations such as streaming of a query results.
 */
public class CancelMessage extends Message.Request
{
    public static final Message.Codec<CancelMessage> codec = new Message.Codec<CancelMessage>()
    {
        public CancelMessage decode(ByteBuf body, int version)
        {
            return new CancelMessage(CBUtil.readUUID(body));
        }

        public void encode(CancelMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeUUID(msg.uuid, dest);
        }

        public int encodedSize(CancelMessage msg, int version)
        {
            return CBUtil.sizeOfUUID(msg.uuid);
        }
    };


    public final UUID uuid;

    public CancelMessage(UUID uuid)
    {
        super(Type.CANCEL);
        this.uuid = uuid;
    }

    public Response execute(QueryState queryState)
    {
        //TODO - implement me
        return null;
    }
}
