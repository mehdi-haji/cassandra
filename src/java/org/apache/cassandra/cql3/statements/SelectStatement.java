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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.db.monitoring.ConstructionTime;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.cql3.async.paging.AsyncPagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 * A number of public methods here are only used internally. However,
 * many of these are made accessible for the benefit of custom
 * QueryHandler implementations, so before reducing their accessibility
 * due consideration should be given.
 */
public class SelectStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);

    private static final int DEFAULT_COUNT_PAGE_SIZE = 10000;
    private final int boundTerms;
    public final CFMetaData cfm;
    public final Parameters parameters;
    public final Selection selection;
    private final Term limit;
    private final Term perPartitionLimit;

    public final StatementRestrictions restrictions;

    private final boolean isReversed;

    /**
     * The comparator used to orders results when multiple keys are selected (using IN).
     */
    private final Comparator<List<ByteBuffer>> orderingComparator;

    private final ColumnFilter queriedColumns;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.emptyMap(), false, false, false);

    public SelectStatement(CFMetaData cfm,
                           int boundTerms,
                           Parameters parameters,
                           Selection selection,
                           StatementRestrictions restrictions,
                           boolean isReversed,
                           Comparator<List<ByteBuffer>> orderingComparator,
                           Term limit,
                           Term perPartitionLimit)
    {
        this.cfm = cfm;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.restrictions = restrictions;
        this.isReversed = isReversed;
        this.orderingComparator = orderingComparator;
        this.parameters = parameters;
        this.limit = limit;
        this.perPartitionLimit = perPartitionLimit;
        this.queriedColumns = gatherQueriedColumns();
    }

    public Iterable<Function> getFunctions()
    {
        List<Function> functions = new ArrayList<>();
        addFunctionsTo(functions);
        return functions;
    }

    private void addFunctionsTo(List<Function> functions)
    {
        selection.addFunctionsTo(functions);
        restrictions.addFunctionsTo(functions);

        if (limit != null)
            limit.addFunctionsTo(functions);

        if (perPartitionLimit != null)
            perPartitionLimit.addFunctionsTo(functions);
    }

    // Note that the queried columns internally is different from the one selected by the
    // user as it also include any column for which we have a restriction on.
    private ColumnFilter gatherQueriedColumns()
    {
        if (selection.isWildcard())
            return ColumnFilter.all(cfm);

        ColumnFilter.Builder builder = ColumnFilter.allColumnsBuilder(cfm);
        // Adds all selected columns
        for (ColumnDefinition def : selection.getColumns())
            if (!def.isPrimaryKeyColumn())
                builder.add(def);
        // as well as any restricted column (so we can actually apply the restriction)
        builder.addAll(restrictions.nonPKRestrictedColumns(true));
        return builder.build();
    }

    /**
     * The columns to fetch internally for this SELECT statement (which can be more than the one selected by the
     * user as it also include any restricted column in particular).
     */
    public ColumnFilter queriedColumns()
    {
        return queriedColumns;
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFMetaData cfm, Selection selection)
    {
        return new SelectStatement(cfm,
                                   0,
                                   defaultParameters,
                                   selection,
                                   StatementRestrictions.empty(StatementType.SELECT, cfm),
                                   false,
                                   null,
                                   null,
                                   null);
    }

    public ResultSet.ResultMetadata getResultMetadata()
    {
        return selection.getResultMetadata(parameters.isJson);
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        if (cfm.isView())
        {
            CFMetaData baseTable = View.findBaseTable(keyspace(), columnFamily());
            if (baseTable != null)
                state.hasColumnFamilyAccess(baseTable, Permission.SELECT);
        }
        else
        {
            state.hasColumnFamilyAccess(cfm, Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensureHasPermission(Permission.EXECUTE, function);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public ResultMessage executeAsync(QueryState state, QueryOptions options)
    throws RequestValidationException, RequestExecutionException
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");
        cl.validateForRead(keyspace());

        if (options.getAsyncPagingOptions().asyncPagingRequested() && state.getConnection() != null)
            return innerExecuteAsync(state, options, FBUtilities.nowInSeconds());
        else
            return innerExecute(state, options, FBUtilities.nowInSeconds(), false);
    }

    public ResultMessage.Rows execute(QueryState state, QueryOptions options)
        throws RequestExecutionException, RequestValidationException
    {
       return innerExecute(state, options,  FBUtilities.nowInSeconds(), false);
    }

    private int getPageSize(QueryOptions options)
    {
        int pageSize = options.getPageSize();

        // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
        // If we user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
        if (selection.isAggregate() && pageSize <= 0)
            pageSize = DEFAULT_COUNT_PAGE_SIZE;

        return  pageSize;
    }

    public ReadQuery getQuery(QueryOptions options, int nowInSec) throws RequestValidationException
    {
        return getQuery(options, nowInSec, getLimit(options), getPerPartitionLimit(options));
    }

    private ReadQuery getQuery(QueryOptions options, int nowInSec, int userLimit, int perPartitionLimit) throws RequestValidationException
    {
        DataLimits limit = getDataLimits(userLimit, perPartitionLimit);
        if (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing())
            return getRangeCommand(options, limit, nowInSec);

        return getSliceCommands(options, limit, nowInSec);
    }

    /**
     * Stores the parameters required to build an Executor and decides which type of executor to build.
     */
    private final static class ExecutorBuilder
    {
        private final SelectStatement statement;
        private final QueryOptions options;
        private final QueryState state;
        private final int nowInSec;
        private final int userLimit;
        private final int userPerPartitionLimit;
        private final ReadQuery query;

        ExecutorBuilder(SelectStatement statement, QueryOptions options, QueryState state, int nowInSec)
        {
            this.statement = statement;
            this.options = options;
            this.state = state;

            this.nowInSec = nowInSec;
            this.userLimit = statement.getLimit(options);
            this.userPerPartitionLimit = statement.getPerPartitionLimit(options);
            this.query = statement.getQuery(options, nowInSec, userLimit, userPerPartitionLimit);
        }

        public Executor build(boolean isInternal)
        {
            if (isInternal || isLocalRangeQuery())
                return new LocalExecutor(this, isInternal);

            return new DistributedExecutor(this);
        }

        /**
         * Check if the query is a range query that can be executed locally,
         * that is the key range is local and the consistency level is ONE or less,
         * and has no index.
         *
         * For single partition queries, we need to worry about read repair and we currently
         * do not handle SinglePartitionReadCommand.Group. For index range queries, we need
         * to worry about index post processing in the coordinator. So we currently only
         * optimize non-indexed range queries, which are the ones that are more likely to
         * retrieve more data.
         */
        private boolean isLocalRangeQuery()
        {
            if (!(query instanceof PartitionRangeReadCommand))
                return false;

            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand)query;

            Keyspace keyspace = Keyspace.open(statement.cfm.ksName);

            if (options.getConsistency().blockFor(keyspace) > 1)
                return false;

            AbstractBounds<PartitionPosition> bounds = statement.restrictions.getPartitionKeyBounds(options);
            if (bounds == null)
                return false; // empty query

            Index index = cmd.getIndex();
            if (index != null)
                return false; // index query

            // currently isLocalRangeQuery() is only called when isInternal = false but
            // just in case we end up here for local only data let's add this safety check
            return statement.cfm.partitioner instanceof LocalPartitioner ||
                   StorageProxy.isLocalRange(keyspace, bounds);
        }
    }

    /**
     * A class for executing queries: ReadQuery and QueryPager are used to read results in different ways
     * and the results are then passed to a Selection.RowBuilder for further processing and filtering.
     */
    public static abstract class Executor
    {
        final SelectStatement statement;
        final QueryOptions options;
        final QueryState state;
        final int nowInSec;
        final int userLimit;
        final ReadQuery query;

        private Executor(ExecutorBuilder builder)
        {
            this.statement = builder.statement;
            this.options = builder.options;
            this.state = builder.state;
            this.nowInSec = builder.nowInSec;
            this.userLimit = builder.userLimit;
            this.query = builder.query;
        }

        /**
         * A interface implemented by the callers of the retrieve page(s) methods, they need
         * to pass in a valid page size and a valid result builder.
         */
        public interface PagingFactory
        {
            /** Given a QueryPager, return a result builder */
            public Selection.RowBuilder builder(QueryPager pager);

            /** Return the page size used by the QueryPager.*/
            public int pageSize();

        }

        /**
         * Execute the entire query without any paging.
         *
         * @param builder - the result builder
         */
        public abstract void retrieveAll(Selection.RowBuilder builder)
        throws RequestValidationException, RequestExecutionException;

        /**
         * Retrieve only a single page of this query.
         *
         * @param factory - a factory class to pass all external parameters that the executor cannot work out
         */
        public abstract void retrieveOnePage(PagingFactory factory)
        throws RequestValidationException, RequestExecutionException;

        /**
         * Retrieve one or more pages, of this query, including all pages.
         *
         * @param factory - a factory class to pass all external parameters that the executor cannot work out
         */
        public abstract void retrieveMultiplePages(PagingFactory factory)
        throws RequestValidationException, RequestExecutionException;

        /**
         * Iterate the results and pass them to the builder by calling statement.processPartition().
         *
         * @param partitions - the partitions to iterate.
         * @throws InvalidRequestException
         */
        void process(PartitionIterator partitions, Selection.RowBuilder builder) throws InvalidRequestException
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    statement.processPartition(partition, options, builder, nowInSec);
                }
            }
        }
    }

    /**
     * An implementation of the executor that executes queries by distributing requests and
     * collecting results via StorageProxy, this is the standard read path.
     */
    private static class DistributedExecutor extends Executor
    {
        private DistributedExecutor(ExecutorBuilder params)
        {
            super(params);

            logger.trace("Created distributed executor");
        }

        public void retrieveAll(Selection.RowBuilder builder) throws RequestValidationException, RequestExecutionException
        {
            try (PartitionIterator data = query.execute(options.getConsistency(), state.getClientState()))
            {
                process(data, builder);
            }

            builder.complete();
        }

        public void retrieveOnePage(PagingFactory factory) throws RequestValidationException, RequestExecutionException
        {
            QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());
            logger.trace("{}.{} - distr retrieveOnePage {}, ps {}",
                         statement.cfm.ksName, statement.cfm.cfName, pager.getClass().getName(), options.getPagingState());

            Selection.RowBuilder builder = factory.builder(pager);
            try (PartitionIterator page = pager.fetchPage(factory.pageSize(), options.getConsistency(), state.getClientState()))
            {
                process(page, builder);
            }

            // Please note that the isExhausted state of the pager only gets updated when we've closed the page, so this
            // shouldn't be moved inside the 'try' above.
            if (!pager.isExhausted())
                builder.setHasMorePages(pager.state());

            builder.complete();
            logger.trace("{}.{} returning ps {}", statement.cfm.ksName, statement.cfm.cfName, pager.state());
        }

        public void retrieveMultiplePages(PagingFactory factory) throws RequestValidationException, RequestExecutionException
        {
            QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());
            Selection.RowBuilder builder = factory.builder(pager);
            while (!pager.isExhausted() && !builder.isCompleted())
            {
                try (PartitionIterator page = pager.fetchPage(factory.pageSize(), options.getConsistency(), state.getClientState()))
                {
                    process(page, builder);
                }
            }

            if (!pager.isExhausted())
                builder.setHasMorePages(pager.state());

            if (!builder.isCompleted())
                builder.complete();
        }
    }

    /**
     * An implementation of the Executor that executes queries only locally, and bypassing StorageProxy.
     * This is a local optimized query used by {@link SelectStatement#executeInternal(QueryState, QueryOptions)}
     * or for very simple local queries that can be optimized not only by bypassing storage proxy but also by
     * "dumping" results into a byte buffer without any reordering, aggregation or transformation to be applied
     * to the columns, basically to extract local data as fast as possible for co-located analytics tools, see
     * CASSANDRA-9259 and CASSANDRA-11521 for more details.
     */
    private static class LocalExecutor extends Executor
    {
        /**
         * The maximum time we keep an iteration open before breaking when the next page boundary is encountered.
         * This is required to ensure we periodically release the execution controller therefore allowing memtables
         * to be flushed and sstables to be deleted if they've been compacted in the meantime.
         */
        private final static long MAX_ITERATION_TIME_MILLIS = 5000;
        private final boolean isInternal;

        private LocalExecutor(ExecutorBuilder params, boolean isInternal)
        {
            super(params);
            this.isInternal = isInternal;

            logger.trace("Created local executor with isInternal {}", isInternal);
        }

        public void retrieveAll(Selection.RowBuilder builder) throws RequestValidationException, RequestExecutionException
        {
            maybeStartMonitoring();

            logger.trace("{}.{} - local retrieveAll - isInternal {}", statement.cfm.ksName, statement.cfm.cfName, isInternal);

            try (ReadExecutionController executionController = query.executionController())
            {
                try (PartitionIterator data = query.executeInternal(executionController))
                {
                    process(data, builder);
                }
            }

            checkMonitoringStatus();
            builder.complete();
        }

        public void retrieveOnePage(PagingFactory factory) throws RequestValidationException, RequestExecutionException
        {
            maybeStartMonitoring();

            QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());
            logger.trace("{}.{} - local retrieveOnePage {} - isInternal {}, ps {}",
                         statement.cfm.ksName, statement.cfm.cfName, pager.getClass().getName(), isInternal, options.getPagingState());

            Selection.RowBuilder builder = factory.builder(pager);
            try (ReadExecutionController executionController = query.executionController();
                 PartitionIterator page = pager.fetchPageInternal(factory.pageSize(), executionController))
            {
                process(page, builder);
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
                throw ex;
            }

            checkMonitoringStatus();

            // Please note that the isExhausted state of the pager only gets updated when we've closed the page, so this
            // shouldn't be moved inside the 'try' above.
            if (!pager.isExhausted())
                builder.setHasMorePages(pager.state());

            builder.complete();
            logger.trace("{}.{}, num rows {}, isExhausted {}", statement.cfm.ksName, statement.cfm.cfName,
                         ((Selection.ResultSetBuilder)builder).build().size(),  pager.isExhausted());
        }

        public void retrieveMultiplePages(PagingFactory factory) throws RequestValidationException, RequestExecutionException
        {
            QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());
            logger.trace("{}.{} - local retrieveMultiplePages {} - isInternal {}",
                         statement.cfm.ksName, statement.cfm.cfName, pager.getClass().getName(), isInternal);

            Selection.RowBuilder builder = factory.builder(pager);
            while (!pager.isExhausted() && !builder.isCompleted())
            {
                long now = ApproximateTime.currentTimeMillis();
                try (ReadExecutionController executionController = query.executionController();
                     PartitionIterator partitions = pager.fetchUpToLimitsInternal(executionController))
                {
                    while (partitions.hasNext())
                    {
                        try (RowIterator data = partitions.next())
                        {
                            statement.processPartition(data, options, builder, nowInSec);
                        }

                        if (ApproximateTime.currentTimeMillis() - now > MAX_ITERATION_TIME_MILLIS)
                        {
                            logger.trace("Pausing long running multiple pages query to release resources");
                            pager.stop(); // cannot just break or pager will think it was exhausted
                        }
                    }
                }
            }

            if (!pager.isExhausted())
                builder.setHasMorePages(pager.state());

            if (!builder.isCompleted())
                builder.complete();
        }

        private void maybeStartMonitoring()
        {
            if (isInternal)
                return;

            logger.trace("{}.{} - Starting query monitoring", statement.cfm.ksName, statement.cfm.cfName);
            query.startMonitoring(new ConstructionTime(System.currentTimeMillis()),
                                  DatabaseDescriptor.getTimeout(MessagingService.Verb.READ));
        }

        private void checkMonitoringStatus() throws RequestValidationException, RequestExecutionException
        {
            if (isInternal)
                return;

            logger.trace("{}.{} - Checking query completion", statement.cfm.ksName, statement.cfm.cfName);
            if (!query.complete())
            {
                throw new ReadFailureException(options.getConsistency(), 0, 1, 1, false);
            }
        }
    }

    private Executor.PagingFactory pagingFactory(final Selection.RowBuilder builder, final int pageSize)
    {
        return new Executor.PagingFactory()
        {
            public Selection.RowBuilder builder(QueryPager pager)
            {
                return builder;
            }

            public int pageSize()
            {
                return pageSize;
            }
        };
    }

    private ResultSet pageQuery(Executor executor, int pageSize)
    throws RequestValidationException, RequestExecutionException
    {
        if (selection.isAggregate())
            return pageAggregateQuery(executor, pageSize);

        // We can't properly do post-query ordering if we page (see #6722)
        checkFalse(needsPostQueryOrdering(),
                   "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                   + " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");

        Selection.ResultSetBuilder builder = selection.resultSetBuilder(executor.options, parameters.isJson);
        executor.retrieveOnePage(pagingFactory(builder, pageSize));
        ResultSet ret = postQueryProcessing(builder, executor.userLimit);

        return ret;
    }

    private ResultSet pageAggregateQuery(Executor executor, int pageSize)
    throws RequestValidationException, RequestExecutionException
    {
        if (!restrictions.hasPartitionKeyRestrictions())
        {
            logger.warn("Aggregation query used without partition key");
            ClientWarn.instance.warn("Aggregation query used without partition key");
        }
        else if (restrictions.keyIsInRelation())
        {
            logger.warn("Aggregation query used on multiple partition keys (IN restriction)");
            ClientWarn.instance.warn("Aggregation query used on multiple partition keys (IN restriction)");
        }

        Selection.ResultSetBuilder builder = selection.resultSetBuilder(executor.options, parameters.isJson);
        executor.retrieveMultiplePages(pagingFactory(builder, pageSize));
        return builder.build();
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        return executeInternal(state, options, FBUtilities.nowInSeconds());
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options, int nowInSec)
    throws RequestExecutionException, RequestValidationException
    {
        return innerExecute(state, options, nowInSec, true);
    }

    /**
     * Execute the query synchronously, typically we only retrieve one page.
     * @param state - the query state
     * @param options - the query options
     * @param isInternal - set to true when we are called from executeInternal, indicates that the query is only local
     * @return - a message containing the result rows
     * @throws RequestExecutionException
     * @throws RequestValidationException
     */
    private ResultMessage.Rows innerExecute(QueryState state, QueryOptions options, int nowInSec, boolean isInternal)
    throws RequestExecutionException, RequestValidationException
    {
        Executor executor = new ExecutorBuilder(this, options, state, nowInSec).build(isInternal);

        int pageSize = getPageSize(options);
        if (pageSize <= 0 || executor.query.limits().count() <= pageSize)
        {
            Selection.ResultSetBuilder builder = selection.resultSetBuilder(options, parameters.isJson);
            executor.retrieveAll(builder);
            return new ResultMessage.Rows(postQueryProcessing(builder, executor.userLimit));
        }

        return new ResultMessage.Rows(pageQuery(executor, pageSize));
    }

    /**
     * Execute the query asynchronously, typically retrieving multiple pages and sending them asynchronously
     * as soon as they become available.
     *
     * @param state - the query state
     * @param options - the query options
     * @return - a void message, the results will be sent asynchronously by the async paging service
     * @throws RequestExecutionException
     * @throws RequestValidationException
     */
    private ResultMessage innerExecuteAsync(QueryState state, QueryOptions options, int nowInSec)
    throws RequestValidationException, RequestExecutionException
    {
        checkFalse(needsPostQueryOrdering(),
                   "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                   + " you must either remove the ORDER BY or the IN and sort client side, or avoid async paging for this query");

        Executor executor = new ExecutorBuilder(this, options, state, nowInSec).build(false);
        StageManager.getStage(Stage.READ).execute(() -> executor.retrieveMultiplePages(AsyncPagingService.pagingFactory(this, state, options)));
        //executor.retrieveMultiplePages(AsyncPagingService.pagingFactory(this, state, options));
        return new ResultMessage.Void();
    }

    public ResultSet process(PartitionIterator partitions, int nowInSec) throws InvalidRequestException
    {
        return process(partitions, QueryOptions.DEFAULT, nowInSec, getLimit(QueryOptions.DEFAULT));
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public Selection getSelection()
    {
        return selection;
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    public StatementRestrictions getRestrictions()
    {
        return restrictions;
    }

    private ReadQuery getSliceCommands(QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options);
        if (keys.isEmpty())
            return ReadQuery.EMPTY;

        ClusteringIndexFilter filter = makeClusteringIndexFilter(options);
        if (filter == null)
            return ReadQuery.EMPTY;

        RowFilter rowFilter = getRowFilter(options);

        // Note that we use the total limit for every key, which is potentially inefficient.
        // However, IN + LIMIT is not a very sensible choice.
        List<SinglePartitionReadCommand> commands = new ArrayList<>(keys.size());
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            DecoratedKey dk = cfm.decorateKey(ByteBufferUtil.clone(key));
            commands.add(SinglePartitionReadCommand.create(cfm, nowInSec, queriedColumns, rowFilter, limit, dk, filter));
        }

        return new SinglePartitionReadCommand.Group(commands, limit);
    }

    /**
     * Returns the slices fetched by this SELECT, assuming an internal call (no bound values in particular).
     * <p>
     * Note that if the SELECT intrinsically selects rows by names, we convert them into equivalent slices for
     * the purpose of this method. This is used for MVs to restrict what needs to be read when we want to read
     * everything that could be affected by a given view (and so, if the view SELECT statement has restrictions
     * on the clustering columns, we can restrict what we read).
     */
    public Slices clusteringIndexFilterAsSlices()
    {
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options);
        if (filter instanceof ClusteringIndexSliceFilter)
            return ((ClusteringIndexSliceFilter)filter).requestedSlices();

        Slices.Builder builder = new Slices.Builder(cfm.comparator);
        for (Clustering clustering: ((ClusteringIndexNamesFilter)filter).requestedRows())
            builder.add(Slice.make(clustering));
        return builder.build();
    }

    /**
     * Returns a read command that can be used internally to query all the rows queried by this SELECT for a
     * give key (used for materialized views).
     */
    public SinglePartitionReadCommand internalReadForView(DecoratedKey key, int nowInSec)
    {
        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        ClusteringIndexFilter filter = makeClusteringIndexFilter(options);
        RowFilter rowFilter = getRowFilter(options);
        return SinglePartitionReadCommand.create(cfm, nowInSec, queriedColumns, rowFilter, DataLimits.NONE, key, filter);
    }

    /**
     * The {@code RowFilter} for this SELECT, assuming an internal call (no bound values in particular).
     */
    public RowFilter rowFilterForInternalCalls()
    {
        return getRowFilter(QueryOptions.forInternalCalls(Collections.emptyList()));
    }

    private ReadQuery getRangeCommand(QueryOptions options, DataLimits limit, int nowInSec) throws RequestValidationException
    {
        ClusteringIndexFilter clusteringIndexFilter = makeClusteringIndexFilter(options);
        if (clusteringIndexFilter == null)
            return ReadQuery.EMPTY;

        RowFilter rowFilter = getRowFilter(options);

        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        AbstractBounds<PartitionPosition> keyBounds = restrictions.getPartitionKeyBounds(options);
        if (keyBounds == null)
            return ReadQuery.EMPTY;

        PartitionRangeReadCommand command = new PartitionRangeReadCommand(cfm,
                                                                          nowInSec,
                                                                          queriedColumns,
                                                                          rowFilter,
                                                                          limit,
                                                                          new DataRange(keyBounds, clusteringIndexFilter),
                                                                          Optional.empty());
        // If there's a secondary index that the command can use, have it validate
        // the request parameters. Note that as a side effect, if a viable Index is
        // identified by the CFS's index manager, it will be cached in the command
        // and serialized during distribution to replicas in order to avoid performing
        // further lookups.
        command.maybeValidateIndex();

        return command;
    }

    private ClusteringIndexFilter makeClusteringIndexFilter(QueryOptions options)
    throws InvalidRequestException
    {
        if (parameters.isDistinct)
        {
            // We need to be able to distinguish between partition having live rows and those that don't. But
            // doing so is not trivial since "having a live row" depends potentially on
            //   1) when the query is performed, due to TTLs
            //   2) how thing reconcile together between different nodes
            // so that it's hard to really optimize properly internally. So to keep it simple, we simply query
            // for the first row of the partition and hence uses Slices.ALL. We'll limit it to the first live
            // row however in getLimit().
            return new ClusteringIndexSliceFilter(Slices.ALL, false);
        }

        if (restrictions.isColumnRange())
        {
            Slices slices = makeSlices(options);
            if (slices == Slices.NONE && !selection.containsStaticColumns())
                return null;

            return new ClusteringIndexSliceFilter(slices, isReversed);
        }
        else
        {
            NavigableSet<Clustering> clusterings = getRequestedRows(options);
            // We can have no clusterings if either we're only selecting the static columns, or if we have
            // a 'IN ()' for clusterings. In that case, we still want to query if some static columns are
            // queried. But we're fine otherwise.
            if (clusterings.isEmpty() && queriedColumns.fetchedColumns().statics.isEmpty())
                return null;

            return new ClusteringIndexNamesFilter(clusterings, isReversed);
        }
    }

    private Slices makeSlices(QueryOptions options)
    throws InvalidRequestException
    {
        SortedSet<ClusteringBound> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, options);
        SortedSet<ClusteringBound> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, options);
        assert startBounds.size() == endBounds.size();

        // The case where startBounds == 1 is common enough that it's worth optimizing
        if (startBounds.size() == 1)
        {
            ClusteringBound start = startBounds.first();
            ClusteringBound end = endBounds.first();
            return cfm.comparator.compare(start, end) > 0
                 ? Slices.NONE
                 : Slices.with(cfm.comparator, Slice.make(start, end));
        }

        Slices.Builder builder = new Slices.Builder(cfm.comparator, startBounds.size());
        Iterator<ClusteringBound> startIter = startBounds.iterator();
        Iterator<ClusteringBound> endIter = endBounds.iterator();
        while (startIter.hasNext() && endIter.hasNext())
        {
            ClusteringBound start = startIter.next();
            ClusteringBound end = endIter.next();

            // Ignore slices that are nonsensical
            if (cfm.comparator.compare(start, end) > 0)
                continue;

            builder.add(start, end);
        }

        return builder.build();
    }

    private DataLimits getDataLimits(int userLimit, int perPartitionLimit)
    {
        int cqlRowLimit = DataLimits.NO_LIMIT;
        int cqlPerPartitionLimit = DataLimits.NO_LIMIT;

        // If we aggregate, the limit really apply to the number of rows returned to the user, not to what is queried, and
        // since in practice we currently only aggregate at top level (we have no GROUP BY support yet), we'll only ever
        // return 1 result and can therefore basically ignore the user LIMIT in this case.
        // Whenever we support GROUP BY, we'll have to add a new DataLimits kind that knows how things are grouped and is thus
        // able to apply the user limit properly.
        // If we do post ordering we need to get all the results sorted before we can trim them.
        if (!selection.isAggregate())
        {
            if (!needsPostQueryOrdering())
                cqlRowLimit = userLimit;
            cqlPerPartitionLimit = perPartitionLimit;
        }
        if (parameters.isDistinct)
            return cqlRowLimit == DataLimits.NO_LIMIT ? DataLimits.DISTINCT_NONE : DataLimits.distinctLimits(cqlRowLimit);

        return DataLimits.cqlLimits(cqlRowLimit, cqlPerPartitionLimit);
    }

    /**
     * Returns the limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    public int getLimit(QueryOptions options)
    {
        return getLimit(limit, options);
    }

    /**
     * Returns the per partition limit specified by the user.
     * May be used by custom QueryHandler implementations
     *
     * @return the per partition limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    private int getPerPartitionLimit(QueryOptions options)
    {
        return getLimit(perPartitionLimit, options);
    }

    private int getLimit(Term limit, QueryOptions options)
    {
        int userLimit = DataLimits.NO_LIMIT;

        if (limit != null)
        {
            ByteBuffer b = checkNotNull(limit.bindAndGet(options), "Invalid null value of limit");
            // treat UNSET limit value as 'unlimited'
            if (b != UNSET_BYTE_BUFFER)
            {
                try
                {
                    Int32Type.instance.validate(b);
                    userLimit = Int32Type.instance.compose(b);
                    checkTrue(userLimit > 0, "LIMIT must be strictly positive");
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException("Invalid limit value");
                }
            }
        }
        return userLimit;
    }

    private NavigableSet<Clustering> getRequestedRows(QueryOptions options) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !restrictions.isColumnRange();
        return restrictions.getClusteringColumns(options);
    }

    /**
     * May be used by custom QueryHandler implementations
     */
    private RowFilter getRowFilter(QueryOptions options) throws InvalidRequestException
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
        SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
        return restrictions.getRowFilter(secondaryIndexManager, options);
    }

    private Selection.ResultSetBuilder getResultSetBuilder(QueryOptions options)
    {
        return selection.resultSetBuilder(options, parameters.isJson);
    }

    private ResultSet process(PartitionIterator partitions,
                              QueryOptions options,
                              int nowInSec,
                              int userLimit) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = getResultSetBuilder(options);
        while (partitions.hasNext())
        {
            try (RowIterator partition = partitions.next())
            {
                processPartition(partition, options, result, nowInSec);
            }
        }

        return postQueryProcessing(result, userLimit);
    }

    private ResultSet postQueryProcessing(Selection.ResultSetBuilder result, int userLimit)
    {
        ResultSet cqlRows = result.build();

        orderResults(cqlRows);

        cqlRows.trim(userLimit);

        return cqlRows;
    }

    public static ByteBuffer[] getComponents(CFMetaData cfm, DecoratedKey dk)
    {
        ByteBuffer key = dk.getKey();
        if (cfm.getKeyValidator() instanceof CompositeType)
        {
            return ((CompositeType)cfm.getKeyValidator()).split(key);
        }
        else
        {
            return new ByteBuffer[]{ key };
        }
    }

    // Used by ModificationStatement for CAS operations
    public void processPartition(RowIterator partition, QueryOptions options, Selection.RowBuilder result, int nowInSec)
    throws InvalidRequestException
    {
        int protocolVersion = options.getProtocolVersion();

        ByteBuffer[] keyComponents = getComponents(cfm, partition.partitionKey());

        Row staticRow = partition.staticRow();
        // If there is no rows, and there's no restriction on clustering/regular columns,
        // then provided the select was a full partition selection (either by partition key and/or by static column),
        // we want to include static columns and we're done.
        if (!partition.hasNext())
        {
            if (!staticRow.isEmpty() && (!restrictions.hasClusteringColumnsRestriction() || cfm.isStaticCompactTable()))
            {
                result.rowStart();
                for (ColumnDefinition def : selection.getColumns())
                {
                    switch (def.kind)
                    {
                        case PARTITION_KEY:
                            result.add(keyComponents[def.position()]);
                            break;
                        case STATIC:
                            addValue(result, def, staticRow, nowInSec, protocolVersion);
                            break;
                        default:
                            result.add(null);
                    }
                }
                result.rowEnd();
            }
            return;
        }

        while (partition.hasNext())
        {
            Row row = partition.next();
            result.rowStart();
            // Respect selection order
            for (ColumnDefinition def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING:
                        result.add(row.clustering().get(def.position()));
                        break;
                    case REGULAR:
                        addValue(result, def, row, nowInSec, protocolVersion);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, nowInSec, protocolVersion);
                        break;
                }
            }
            result.rowEnd();
        }
    }

    private static void addValue(Selection.RowBuilder result, ColumnDefinition def, Row row, int nowInSec, int protocolVersion)
    {
        if (def.isComplex())
        {
            assert def.type.isMultiCell();
            ComplexColumnData complexData = row.getComplexColumnData(def);
            if (complexData == null)
                result.add(null);
            else if (def.type.isCollection())
                result.add(((CollectionType) def.type).serializeForNativeProtocol(complexData.iterator(), protocolVersion));
            else
                result.add(((UserType) def.type).serializeForNativeProtocol(complexData.iterator(), protocolVersion));
        }
        else
        {
            result.add(row.getCell(def), nowInSec);
        }
    }

    private boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return restrictions.keyIsInRelation() && !parameters.orderings.isEmpty();
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        Collections.sort(cqlRows.rows, orderingComparator);
    }

    public static class RawStatement extends CFStatement
    {
        public final Parameters parameters;
        public final List<RawSelector> selectClause;
        public final WhereClause whereClause;
        public final Term.Raw limit;
        public final Term.Raw perPartitionLimit;

        public RawStatement(CFName cfName, Parameters parameters,
                            List<RawSelector> selectClause,
                            WhereClause whereClause,
                            Term.Raw limit,
                            Term.Raw perPartitionLimit)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause;
            this.limit = limit;
            this.perPartitionLimit = perPartitionLimit;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            return prepare(false);
        }

        public ParsedStatement.Prepared prepare(boolean forView) throws InvalidRequestException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();

            Selection selection = selectClause.isEmpty()
                                  ? Selection.wildcard(cfm)
                                  : Selection.fromSelectors(cfm, selectClause, boundNames);

            StatementRestrictions restrictions = prepareRestrictions(cfm, boundNames, selection, forView);

            if (parameters.isDistinct)
            {
                checkNull(perPartitionLimit, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
                validateDistinctSelection(cfm, selection, restrictions);
            }

            checkFalse(selection.isAggregate() && perPartitionLimit != null,
                       "PER PARTITION LIMIT is not allowed with aggregate queries.");

            Comparator<List<ByteBuffer>> orderingComparator = null;
            boolean isReversed = false;

            if (!parameters.orderings.isEmpty())
            {
                assert !forView;
                verifyOrderingIsAllowed(restrictions);
                orderingComparator = getOrderingComparator(cfm, selection, restrictions);
                isReversed = isReversed(cfm);
                if (isReversed)
                    orderingComparator = Collections.reverseOrder(orderingComparator);
            }

            checkNeedsFiltering(restrictions);

            SelectStatement stmt = new SelectStatement(cfm,
                                                        boundNames.size(),
                                                        parameters,
                                                        selection,
                                                        restrictions,
                                                        isReversed,
                                                        orderingComparator,
                                                        prepareLimit(boundNames, limit, keyspace(), limitReceiver()),
                                                        prepareLimit(boundNames, perPartitionLimit, keyspace(), perPartitionLimitReceiver()));

            return new ParsedStatement.Prepared(stmt, boundNames, boundNames.getPartitionKeyBindIndexes(cfm));
        }

        /**
         * Prepares the restrictions.
         *
         * @param cfm the column family meta data
         * @param boundNames the variable specifications
         * @param selection the selection
         * @return the restrictions
         * @throws InvalidRequestException if a problem occurs while building the restrictions
         */
        private StatementRestrictions prepareRestrictions(CFMetaData cfm,
                                                          VariableSpecifications boundNames,
                                                          Selection selection,
                                                          boolean forView) throws InvalidRequestException
        {
            return new StatementRestrictions(StatementType.SELECT,
                                             cfm,
                                             whereClause,
                                             boundNames,
                                             selection.containsOnlyStaticColumns(),
                                             selection.containsAComplexColumn(),
                                             parameters.allowFiltering,
                                             forView);
        }

        /** Returns a Term for the limit or null if no limit is set */
        private Term prepareLimit(VariableSpecifications boundNames, Term.Raw limit,
                                  String keyspace, ColumnSpecification limitReceiver) throws InvalidRequestException
        {
            if (limit == null)
                return null;

            Term prepLimit = limit.prepare(keyspace, limitReceiver);
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
        }

        private static void verifyOrderingIsAllowed(StatementRestrictions restrictions) throws InvalidRequestException
        {
            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported.");
            checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private static void validateDistinctSelection(CFMetaData cfm,
                                                      Selection selection,
                                                      StatementRestrictions restrictions)
                                                      throws InvalidRequestException
        {
            checkFalse(restrictions.hasClusteringColumnsRestriction() ||
                       (restrictions.hasNonPrimaryKeyRestrictions() && !restrictions.nonPKRestrictedColumns(true).stream().allMatch(ColumnDefinition::isStatic)),
                       "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.");

            Collection<ColumnDefinition> requestedColumns = selection.getColumns();
            for (ColumnDefinition def : requestedColumns)
                checkFalse(!def.isPartitionKey() && !def.isStatic(),
                           "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                           def.name);

            // If it's a key range, we require that all partition key columns are selected so we don't have to bother
            // with post-query grouping.
            if (!restrictions.isKeyRange())
                return;

            for (ColumnDefinition def : cfm.partitionKeyColumns())
                checkTrue(requestedColumns.contains(def),
                          "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name);
        }

        private Comparator<List<ByteBuffer>> getOrderingComparator(CFMetaData cfm,
                                                                   Selection selection,
                                                                   StatementRestrictions restrictions)
                                                                   throws InvalidRequestException
        {
            if (!restrictions.keyIsInRelation())
                return null;

            Map<ColumnIdentifier, Integer> orderingIndexes = getOrderingIndex(cfm, selection);

            List<Integer> idToSort = new ArrayList<Integer>();
            List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>();

            for (ColumnDefinition.Raw raw : parameters.orderings.keySet())
            {
                ColumnDefinition orderingColumn = raw.prepare(cfm);
                idToSort.add(orderingIndexes.get(orderingColumn.name));
                sorters.add(orderingColumn.type);
            }
            return idToSort.size() == 1 ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                    : new CompositeComparator(sorters, idToSort);
        }

        private Map<ColumnIdentifier, Integer> getOrderingIndex(CFMetaData cfm, Selection selection)
                throws InvalidRequestException
        {
            // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
            // even if we don't
            // ultimately ship them to the client (CASSANDRA-4911).
            Map<ColumnIdentifier, Integer> orderingIndexes = new HashMap<>();
            for (ColumnDefinition.Raw raw : parameters.orderings.keySet())
            {
                final ColumnDefinition def = raw.prepare(cfm);
                int index = selection.getResultSetIndex(def);
                if (index < 0)
                    index = selection.addColumnForOrdering(def);
                orderingIndexes.put(def.name, index);
            }
            return orderingIndexes;
        }

        private boolean isReversed(CFMetaData cfm) throws InvalidRequestException
        {
            Boolean[] reversedMap = new Boolean[cfm.clusteringColumns().size()];
            int i = 0;
            for (Map.Entry<ColumnDefinition.Raw, Boolean> entry : parameters.orderings.entrySet())
            {
                ColumnDefinition def = entry.getKey().prepare(cfm);
                boolean reversed = entry.getValue();

                checkTrue(def.isClusteringColumn(),
                          "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", def.name);

                checkTrue(i++ == def.position(),
                          "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");

                reversedMap[def.position()] = (reversed != def.isReversedType());
            }

            // Check that all boolean in reversedMap, if set, agrees
            Boolean isReversed = null;
            for (Boolean b : reversedMap)
            {
                // Column on which order is specified can be in any order
                if (b == null)
                    continue;

                if (isReversed == null)
                {
                    isReversed = b;
                    continue;
                }
                checkTrue(isReversed.equals(b), "Unsupported order by relation");
            }
            assert isReversed != null;
            return isReversed;
        }

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(StatementRestrictions restrictions) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing()))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the row filter is not the identity
                checkFalse(restrictions.needFiltering(), StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
            }
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        private ColumnSpecification perPartitionLimitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[per_partition_limit]", true), Int32Type.instance);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("name", cfName)
                              .add("selectClause", selectClause)
                              .add("whereClause", whereClause)
                              .add("isDistinct", parameters.isDistinct)
                              .toString();
        }
    }

    public static class Parameters
    {
        // Public because CASSANDRA-9858
        public final Map<ColumnDefinition.Raw, Boolean> orderings;
        public final boolean isDistinct;
        public final boolean allowFiltering;
        public final boolean isJson;

        public Parameters(Map<ColumnDefinition.Raw, Boolean> orderings,
                          boolean isDistinct,
                          boolean allowFiltering,
                          boolean isJson)
        {
            this.orderings = orderings;
            this.isDistinct = isDistinct;
            this.allowFiltering = allowFiltering;
            this.isJson = isJson;
        }
    }

    private static abstract class ColumnComparator<T> implements Comparator<T>
    {
        protected final int compare(Comparator<ByteBuffer> comparator, ByteBuffer aValue, ByteBuffer bValue)
        {
            if (aValue == null)
                return bValue == null ? 0 : -1;

            return bValue == null ? 1 : comparator.compare(aValue, bValue);
        }
    }

    /**
     * Used in orderResults(...) method when single 'ORDER BY' condition where given
     */
    private static class SingleColumnComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final int index;
        private final Comparator<ByteBuffer> comparator;

        public SingleColumnComparator(int columnIndex, Comparator<ByteBuffer> orderer)
        {
            index = columnIndex;
            comparator = orderer;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            return compare(comparator, a.get(index), b.get(index));
        }
    }

    /**
     * Used in orderResults(...) method when multiple 'ORDER BY' conditions where given
     */
    private static class CompositeComparator extends ColumnComparator<List<ByteBuffer>>
    {
        private final List<Comparator<ByteBuffer>> orderTypes;
        private final List<Integer> positions;

        private CompositeComparator(List<Comparator<ByteBuffer>> orderTypes, List<Integer> positions)
        {
            this.orderTypes = orderTypes;
            this.positions = positions;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            for (int i = 0; i < positions.size(); i++)
            {
                Comparator<ByteBuffer> type = orderTypes.get(i);
                int columnPos = positions.get(i);

                int comparison = compare(type, a.get(columnPos), b.get(columnPos));

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }
}
