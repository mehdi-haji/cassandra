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
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.AbstractIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;

/**
 *
 * TODO: THIS CLASS IS NO LONGER USED AND CAN BE DELETED once the code reviewer has accepted PartitionGroupQueryPager
 * as a valid alternative. I leave it here as I plan to squash the commits before submitting for review, and I do
 * not want to loose the modifications I made to it in order to make it work with the modifications done
 * to AbstractQueryPager.
 *
 * --
 *
 * Pager over a list of ReadCommand.
 *
 * Note that this is not easy to make efficient. Indeed, we need to page the first command fully before
 * returning results from the next one, but if the result returned by each command is small (compared to pageSize),
 * paging the commands one at a time under-performs compared to parallelizing. On the other, if we parallelize
 * and each command raised pageSize results, we'll end up with commands.size() * pageSize results in memory, which
 * defeats the purpose of paging.
 *
 * For now, we keep it simple (somewhat) and just do one command at a time. Provided that we make sure to not
 * create a pager unless we need to, this is probably fine. Though if we later want to get fancy, we could use the
 * cfs meanPartitionSize to decide if parallelizing some of the command might be worth it while being confident we don't
 * blow out memory.
 */
public class MultiPartitionPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(MultiPartitionPager.class);

    private final int protocolVersion;
    private final DataLimits limit;
    private final int nowInSec;
    private final List<SinglePartitionReadCommand> commands;
    private SinglePartitionPager current;
    private PagersIterator iter;

    public MultiPartitionPager(SinglePartitionReadCommand.Group group, PagingState state, int protocolVersion)
    {
        this.protocolVersion = protocolVersion;
        this.limit = group.limits();
        this.nowInSec = group.nowInSec();

        int i = 0;
        // If it's not the beginning (state != null), we need to find where we were and skip previous commands
        // since they are done.
        if (state != null)
            for (; i < group.commands.size(); i++)
                if (group.commands.get(i).partitionKey().getKey().equals(state.partitionKey))
                    break;

        if (i >= group.commands.size())
        {
            commands = Collections.emptyList();
            current = SinglePartitionPager.empty(group.commands.get(i - 1), state, protocolVersion);
            assert current.isExhausted();
            return;
        }

        commands = new ArrayList<>(group.commands.size() - i);
        commands.addAll(group.commands.subList(i, group.commands.size()));
        current = nextPager(state);

        logger.info("Created MultiPartitionPager with {} commands, state {}, current {}", group.commands.size(), state, current.command().partitionKey());
    }

    private SinglePartitionPager nextPager(PagingState state)
    {
        if (commands.isEmpty())
            return current;

        SinglePartitionReadCommand command = commands.remove(0);
        PagingState nextState = current == null
                                ? state // if no current accept the state from the client, otherwise we are transitioning into a new partition
                                : new PagingState(command.partitionKey().getKey(), null, current.maxRemaining(), limit.perPartitionCount());
        SinglePartitionPager ret = command.getPager(nextState, protocolVersion);
        logger.info("New pager for key {}", ret.command().partitionKey());
        return ret;
    }

    public PagingState state()
    {
        if (isExhausted())
            return null;

        PagingState state = current.state();
        PagingState ret = new PagingState(current.key(), state == null ? null : state.rowMark, current.maxRemaining(), current.remainingInPartition());
        logger.info("Returning state: {}", ret);
        return ret;
    }

    public boolean isExhausted()
    {
        logger.debug("IsExhausted: commands left {}, current exhausted {}", commands.size(), current.isExhausted());
        return commands.isEmpty() && current.isExhausted();
    }

    public void saveState()
    {
        if (iter != null)
            iter.counted += current.counted();

        current.saveState();
        logger.info("State saved: {}, {}, {}", current.state(), current.isExhausted(), current.maxRemaining());
    }

    public void stop()
    {
        current.stop();
    }

    public ReadExecutionController executionController()
    {
        // Note that for all pagers, the only difference is the partition key to which it applies, so in practice we
        // can use any of the sub-pager ReadOrderGroup group to protect the whole pager
        if (current != null)
            return current.executionController();

        throw new AssertionError("Shouldn't be called on an exhausted pager");
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState) throws RequestValidationException, RequestExecutionException
    {
        maybeCloseIter();

        int toQuery = Math.min(current.maxRemaining(), pageSize);
        iter = new PagersIterator(toQuery, (p, n) -> p.fetchPage(n, consistency, clientState));
        return iter;
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchPageInternal(int pageSize, ReadExecutionController executionController) throws RequestValidationException, RequestExecutionException
    {
        maybeCloseIter();

        int toQuery = Math.min(current.maxRemaining(), pageSize);
        iter = new PagersIterator(toQuery, (p, n) -> p.fetchPageInternal(n, executionController));
        return iter;
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchUpToLimitsInternal(ReadExecutionController executionController)
    throws RequestValidationException, RequestExecutionException
    {
        maybeCloseIter();

        iter = new PagersIterator(current.maxRemaining(), (p, n) -> p.fetchUpToLimitsInternal(executionController));
        return iter;
    }

    private void maybeCloseIter()
    {
        if (iter != null)
            iter.close();
    }

    private class PagersIterator extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final int pageSize;
        private PartitionIterator result;
        private int counted;

        // A function that given a pager and how many rows to query, it will return the next partition iterator
        private BiFunction<QueryPager, Integer, PartitionIterator> partitionItFunction;

        public PagersIterator(int pageSize, BiFunction<QueryPager, Integer, PartitionIterator> partitionItFunction)
        {
            this.pageSize = pageSize;
            this.partitionItFunction = partitionItFunction;
        }

        protected RowIterator computeNext()
        {
            while (result == null || !result.hasNext())
            {
                if (result != null)
                {
                    counted += current.counted(); // it's important to get the count before closing
                    result.close();
                    logger.info("Result exhausted with {} counted, state {}, exhausted {}", counted, current.state(), current.isExhausted());
                }

                if (current.isExhausted())
                { //if the current is exhausted see if the next pager has more results
                    current = nextPager(current.state());
                    if (isExhausted())
                        return endOfData();
                }

                int toQuery = Math.min(current.maxRemaining(), pageSize - counted);
                if (toQuery == 0)
                    return endOfData();

                logger.info("Querying {} for max {} rows", current.command().partitionKey(), toQuery);
                result = partitionItFunction.apply(current, toQuery);
            }
            return result.next();
        }

        public void close()
        {
            if (result != null)
                result.close();
        }
    }

    public int maxRemaining()
    {
        return current.maxRemaining();
    }
}
