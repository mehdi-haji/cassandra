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

import java.util.Arrays;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.cql3.async.paging.AsyncPagingService;

/**
 * A request to cancel a long running operations.
 */
public class CancelMessage extends Message.Request
{
    public enum OperationType
    {
        ASYNC_PAGING(1);

        private final int id;
        OperationType(int id)
        {
            this.id = id;
        }

        private static final OperationType[] operationTypes;
        static
        {
            int maxId = Arrays.stream(OperationType.values()).map(ot -> ot.id).reduce(0, Math::max);
            operationTypes = new OperationType[maxId + 1];
            for (OperationType ot : OperationType.values())
            {
                if (operationTypes[ot.id] != null)
                    throw new IllegalStateException("Duplicate operation type");
                operationTypes[ot.id] = ot;
            }
        }

        static OperationType decode(int id)
        {
            if (id >= operationTypes.length || operationTypes[id] == null)
                throw new ProtocolException(String.format("Unknown operation type %d", id));
            return operationTypes[id];
        }
    }

    public static final Message.Codec<CancelMessage> codec = new Message.Codec<CancelMessage>()
    {
        public CancelMessage decode(ByteBuf body, int version)
        {
            return new CancelMessage(OperationType.decode(body.readInt()), CBUtil.readUUID(body));
        }

        public void encode(CancelMessage msg, ByteBuf dest, int version)
        {
            dest.writeInt(msg.operationType.id);
            CBUtil.writeUUID(msg.uuid, dest);
        }

        public int encodedSize(CancelMessage msg, int version)
        {
            return 4 + CBUtil.sizeOfUUID(msg.uuid);
        }
    };

    /** The type of the operation to cancel */
    private final OperationType operationType;

    /** The unique identifier of the operation to cancel */
    private final UUID uuid;

    private CancelMessage(OperationType operationType, UUID uuid)
    {
        super(Type.CANCEL);
        this.operationType = operationType;
        this.uuid = uuid;
    }

    public Response execute(QueryState queryState)
    {
        if (operationType == OperationType.ASYNC_PAGING)
            AsyncPagingService.cancel(uuid);
        else
            throw new InvalidRequestException(String.format("Unknown operation type: %s", operationType));

        return new ResultMessage.Void();
    }
}
