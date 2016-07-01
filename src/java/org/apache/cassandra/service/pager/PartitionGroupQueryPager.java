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
package org.apache.cassandra.service.pager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;

/**
 *
 */
public class PartitionGroupQueryPager extends AbstractQueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionGroupQueryPager.class);

    private final SinglePartitionReadCommand.Group group;
    private volatile DecoratedKey lastReturnedKey;
    private volatile PagingState.RowMark lastReturnedRow;

    public PartitionGroupQueryPager(SinglePartitionReadCommand.Group group, PagingState state, int protocolVersion)
    {
        super(group, protocolVersion);
        this.group = group;

        if (state != null)
        {
            lastReturnedKey = group.metadata().decorateKey(state.partitionKey);
            lastReturnedRow = state.rowMark;
            restoreState(lastReturnedKey, state.remaining, state.remainingInPartition);
        }

        logger.debug("Created PartitionGroupQueryPager with {} commands, state {}", group.commands.size(), state);
    }

    /**
     * @return the paging state, {@link this#recordLast(DecoratedKey, Row)} should have been previously called.
     */
    public PagingState state()
    {
        return lastReturnedKey == null
               ? null
               : new PagingState(lastReturnedKey.getKey(), lastReturnedRow, maxRemaining(), remainingInPartition());
    }

    protected PartitionIterator executeCommand(int pageSize, ConsistencyLevel consistency, ClientState clientState)
    throws RequestExecutionException
    {
        return innerExecute(pageSize, command -> command.execute(consistency, clientState));
    }

    protected PartitionIterator executeCommandInternal(int pageSize, ReadExecutionController executionController)
    throws RequestExecutionException
    {
        return innerExecute(pageSize, command -> command.executeInternal(executionController));
    }

    /**
     * Implementation of the execute methods. If there are no valid commands return an empty partition iterator.
     * Otherwise return a partition iterator for the first command and extensible with the following commands,
     * see {@link MorePartitions#extend(PartitionIterator, MorePartitions)}.
     *
     * @param pageSize - the page size
     * @param iterSupplier - a function that given a read command, calls the correct execute method on the command to return the partition iterator
     * @return - a partition iterator that can be extended with all sub-sequent partition iterators of other cammands.
     *
     */
    private PartitionIterator innerExecute(final int pageSize, final Function<ReadCommand, PartitionIterator> iterSupplier)
    throws RequestExecutionException
    {
        List<SinglePartitionReadCommand> commands = commands();
        if (commands.isEmpty())
            return EmptyIterators.partition();

        class Extend implements MorePartitions<PartitionIterator>
        {
            private int i = 1;
            public PartitionIterator moreContents()
            {
                if (i >= commands.size())
                    return null;
                return iterSupplier.apply(nextPageReadCommand(commands.get(i++), pageSize));
            }
        }

        return MorePartitions.extend(iterSupplier.apply(nextPageReadCommand(commands.get(0), pageSize)), new Extend());
    }

    /**
     * Select the commands that are applicable by comparing with the last returned key, if any. When we find
     * a command with the same key, return this command and all following commands but, if remaining in partition
     * is <= 0, then skip the command with the same key since it means we had processed the entire partition
     * (important when the partition has no clustering columns that we can use as bookmark).
     *
     * @return A sub-list of commands that we must query
     */
    private List<SinglePartitionReadCommand> commands()
    {
        logger.trace("Working out next command with {} and {} remaining in partition", lastReturnedKey, remainingInPartition());

        int i = 0;
        if (lastReturnedKey != null)
            for (; i < group.commands.size(); i++)
                if (group.commands.get(i).partitionKey().equals(lastReturnedKey))
                    break;

        if (remainingInPartition() <= 0)
            i++; // no need to check this partition

        logger.trace("Command chosen is no. {}", i);

        if (i >= group.commands.size())
            return Collections.emptyList();

        List<SinglePartitionReadCommand> ret = new ArrayList<>(group.commands.size() - i);
        ret.addAll(group.commands.subList(i, group.commands.size()));
        return ret;
    }

    /**
     * Convert a read command into one tha is suitable for querying the specified page size.
     * If the command key is the same as the last returned key, then make sure the command only
     * starts at the last returned row, if any. Otherwise start from the beginning. Also, if resuming
     * from a previous row, make sure to observer the remaining in partition limit.
     *
     * @param command - the original read command
     * @param pageSize - the pages size
     * @return a command that will start at the correct last returned row and apply the pageSize as a limit.
     */
    private ReadCommand nextPageReadCommand(SinglePartitionReadCommand command, int pageSize)
    {
        DecoratedKey key = command.partitionKey();
        PagingState.RowMark row = Objects.equals(key, lastReturnedKey) ? lastReturnedRow : null;

        Clustering clustering = row == null ? null : row.clustering(command.metadata());
        DataLimits commandLimits = (row == null || command.isForThrift())
                                   ? limits.forPaging(pageSize)
                                   : limits.forPaging(pageSize, key.getKey(), remainingInPartition());

        return command.forPaging(clustering, commandLimits);
    }

    /**
     * Save the last returned key and row. This is called when the base class is saving its internal
     * state in preparatio for retrieving a valid paging state. This currently happens when the partition
     * iterator is closed or when the saveState() is called manually.
     *
     * @param key - the last returned key
     * @param row - the last returned row
     */
    protected void recordLast(DecoratedKey key, Row row)
    {

        lastReturnedKey = key;
        if (row != null && row.clustering() != Clustering.STATIC_CLUSTERING)
            lastReturnedRow = PagingState.RowMark.create(group.metadata(), row, protocolVersion);

        logger.trace("Recording last values: {}, {}", lastReturnedKey, lastReturnedRow);
    }

    protected boolean isPreviouslyReturnedPartition(DecoratedKey key)
    {
        // Note that lastReturnedKey can be null, but key cannot.
        return key.equals(lastReturnedKey);
    }


}
