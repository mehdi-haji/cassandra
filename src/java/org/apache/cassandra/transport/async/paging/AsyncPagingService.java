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
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
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
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A class for streaming the entire results of a query to the client.
 * Each streaming session has a unique identifier (uuid).
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

    private final static class ReadRunnable implements Runnable
    {
        private final UUID uuid;
        private final SelectStatement statement;
        private final QueryState state;
        private final QueryOptions options;
        private final CFMetaData cfm;
        private final int nowInSec;
        private final ReadQuery query;
        private final AsyncPages output;

        private final Selection.ResultSetBuilder builder;
        private int seqNo = 1;

        ReadRunnable(SelectStatement statement,
                     QueryState state,
                     QueryOptions options)
        {
            this.uuid = options.getAsyncPagingOptions().uuid;
            this.statement = statement;
            this.state = state;
            this.options = options;
            this.cfm = statement.cfm;
            this.nowInSec = FBUtilities.nowInSeconds();
            this.query = statement.getQuery(options, nowInSec);
            this.builder = statement.getResultSetBuilder();
            this.output = new AsyncPages(state.getConnection());
        }

        public void run() throws RequestExecutionException, RequestValidationException
        {
            int pageSize = statement.getPageSize(options);

            try (ReadExecutionController executionController = query.executionController())
            {
                QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());
                try (PartitionIterator partitions = pager.fetchMultiplePagesInternal(pageSize, executionController))
                {
                    while (partitions.hasNext())
                    {
                        try (BaseRowIterator data = partitions.next())
                        {
                            processPartition(data, pager);
                        }
                    }
                }

                sendCurrentResult(pager);

            }
            catch (Throwable ex)
            {
                JVMStabilityInspector.inspectCommitLogThrowable(ex);
                logger.error(ex.getMessage());
            }
        }

        void processPartition(BaseRowIterator partition, QueryPager pager) throws InvalidRequestException
        {
            int protocolVersion = options.getProtocolVersion();

            ByteBuffer[] keyComponents = SelectStatement.getComponents(cfm, partition.partitionKey());

            Row staticRow = partition.staticRow();
            // If there is no rows, and there's no restriction on clustering/regular columns,
            // then provided the select was a full partition selection (either by partition key and/or by static column),
            // we want to include static columns and we're done.
            if (!partition.hasNext())
            {
                if (!staticRow.isEmpty() && (!statement.restrictions.hasClusteringColumnsRestriction() || cfm.isStaticCompactTable()))
                    builder.addStaticRow(staticRow, keyComponents, statement.selection.getColumns(), nowInSec, protocolVersion);

                if (pager.checkPageBoundaries())
                    sendCurrentResult(pager);
                return;
            }

            while (partition.hasNext())
            {
                builder.addRow((Row) partition.next(), staticRow, keyComponents, statement.selection.getColumns(), nowInSec, protocolVersion);
                if (pager.checkPageBoundaries())
                    sendCurrentResult(pager);
            }
        }

        private void sendCurrentResult(QueryPager pager)
        {
            ResultSet resultSet = builder.build(options.getProtocolVersion());
            statement.orderResults(resultSet);
            resultSet.trim(statement.getLimit(options));

            if (!pager.isExhausted())
                resultSet.metadata.setHasMorePages(pager.state());
            resultSet.metadata.setStreamingState(new AsyncPagingParams(uuid, seqNo));

            logger.debug("Pushing result: {}, {}, {}, {}", seqNo, resultSet.rows.size(), pager.maxRemaining(), uuid);
            while (true) //TODO - fixme
            {
                if (output.sendPage(resultSet, 5000))
                    break;
            }

            if (seqNo == 1)
                state.getConnection().channel().writeAndFlush(output);

            pager.reset();
            builder.reset();
            seqNo++;
        }
    }
}
