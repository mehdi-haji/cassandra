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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.Pair;

/**
 * Options for a query.
 */
public abstract class QueryOptions
{
    public static final QueryOptions DEFAULT = new DefaultQueryOptions(ConsistencyLevel.ONE,
                                                                       Collections.<ByteBuffer>emptyList(),
                                                                       false,
                                                                       SpecificOptions.DEFAULT,
                                                                       AsyncPagingOptions.DEFAULT,
                                                                       Server.CURRENT_VERSION);

    public static final CBCodec<QueryOptions> codec = new Codec();

    // A cache of bind values parsed as JSON, see getJsonColumnValue for details.
    private List<Map<ColumnIdentifier, Term>> jsonValuesCache;

    public static QueryOptions fromThrift(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, AsyncPagingOptions.DEFAULT, Server.VERSION_3);
    }

    public static QueryOptions forInternalCalls(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, AsyncPagingOptions.DEFAULT, Server.VERSION_3);
    }

    public static QueryOptions forInternalCalls(List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(ConsistencyLevel.ONE, values, false, SpecificOptions.DEFAULT, AsyncPagingOptions.DEFAULT, Server.VERSION_3);
    }

    public static QueryOptions forProtocolVersion(int protocolVersion)
    {
        return new DefaultQueryOptions(null, null, true, null, null, protocolVersion);
    }

    public static QueryOptions create(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, int pageSize, PagingState pagingState, ConsistencyLevel serialConsistency)
    {
        return new DefaultQueryOptions(consistency, values, skipMetadata, new SpecificOptions(pageSize, pagingState, serialConsistency, -1L), AsyncPagingOptions.DEFAULT, 0);
    }

    public static QueryOptions addColumnSpecifications(QueryOptions options, List<ColumnSpecification> columnSpecs)
    {
        return new OptionsWithColumnSpecifications(options, columnSpecs);
    }

    public abstract ConsistencyLevel getConsistency();
    public abstract List<ByteBuffer> getValues();
    public abstract boolean skipMetadata();

    /**
     * Returns the term corresponding to column {@code columnName} in the JSON value of bind index {@code bindIndex}.
     *
     * This is functionally equivalent to:
     *   {@code Json.parseJson(UTF8Type.instance.getSerializer().deserialize(getValues().get(bindIndex)), expectedReceivers).get(columnName)}
     * but this cache the result of parsing the JSON so that while this might be called for multiple columns on the same {@code bindIndex}
     * value, the underlying JSON value is only parsed/processed once.
     *
     * Note: this is a bit more involved in CQL specifics than this class generally is but we as we need to cache this per-query and in an object
     * that is available when we bind values, this is the easier place to have this.
     *
     * @param bindIndex the index of the bind value that should be interpreted as a JSON value.
     * @param columnName the name of the column we want the value of.
     * @param expectedReceivers the columns expected in the JSON value at index {@code bindIndex}. This is only used when parsing the
     * json initially and no check is done afterwards. So in practice, any call of this method on the same QueryOptions object and with the same
     * {@code bindIndx} values should use the same value for this parameter, but this isn't validated in any way.
     *
     * @return the value correspong to column {@code columnName} in the (JSON) bind value at index {@code bindIndex}. This may return null if the
     * JSON value has no value for this column.
     */
    public Term getJsonColumnValue(int bindIndex, ColumnIdentifier columnName, Collection<ColumnDefinition> expectedReceivers) throws InvalidRequestException
    {
        if (jsonValuesCache == null)
            jsonValuesCache = new ArrayList<>(Collections.<Map<ColumnIdentifier, Term>>nCopies(getValues().size(), null));

        Map<ColumnIdentifier, Term> jsonValue = jsonValuesCache.get(bindIndex);
        if (jsonValue == null)
        {
            ByteBuffer value = getValues().get(bindIndex);
            if (value == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            jsonValue = Json.parseJson(UTF8Type.instance.getSerializer().deserialize(value), expectedReceivers);
            jsonValuesCache.set(bindIndex, jsonValue);
        }

        return jsonValue.get(columnName);
    }

    /**
     * Tells whether or not this <code>QueryOptions</code> contains the column specifications for the bound variables.
     * <p>The column specifications will be present only for prepared statements.</p>
     * @return <code>true</code> this <code>QueryOptions</code> contains the column specifications for the bound
     * variables, <code>false</code> otherwise.
     */
    public boolean hasColumnSpecifications()
    {
        return false;
    }

    /**
     * Returns the column specifications for the bound variables (<i>optional operation</i>).
     *
     * <p>The column specifications will be present only for prepared statements.</p>
     *
     * <p>Invoke the {@link hasColumnSpecifications} method before invoking this method in order to ensure that this
     * <code>QueryOptions</code> contains the column specifications.</p>
     *
     * @return the option names
     * @throws UnsupportedOperationException If this <code>QueryOptions</code> does not contains the column
     * specifications.
     */
    public ImmutableList<ColumnSpecification> getColumnSpecifications()
    {
        throw new UnsupportedOperationException();
    }

    /**  The pageSize for this query. Will be {@code <= 0} if not relevant for the query.  */
    public int getPageSize()
    {
        return getSpecificOptions().pageSize;
    }

    /** The paging state for this query, or null if not relevant. */
    public PagingState getPagingState()
    {
        return getSpecificOptions().state;
    }

    /**  Serial consistency for conditional updates. */
    public ConsistencyLevel getSerialConsistency()
    {
        return getSpecificOptions().serialConsistency;
    }

    public long getTimestamp(QueryState state)
    {
        long tstamp = getSpecificOptions().timestamp;
        return tstamp != Long.MIN_VALUE ? tstamp : state.getTimestamp();
    }

    /**
     * The protocol version for the query. Will be 3 if the object don't come from
     * a native protocol request (i.e. it's been allocated locally or by CQL-over-thrift).
     */
    public abstract int getProtocolVersion();

    // Mainly for the sake of BatchQueryOptions
    abstract SpecificOptions getSpecificOptions();

    // Streaming options
    public abstract AsyncPagingOptions getAsyncPagingOptions();

    public QueryOptions prepare(List<ColumnSpecification> specs)
    {
        return this;
    }

    static class DefaultQueryOptions extends QueryOptions
    {
        private final ConsistencyLevel consistency;
        private final List<ByteBuffer> values;
        private final boolean skipMetadata;

        private final SpecificOptions options;
        private final AsyncPagingOptions asyncPagingOptions;

        private final transient int protocolVersion;

        DefaultQueryOptions(ConsistencyLevel consistency,
                            List<ByteBuffer> values,
                            boolean skipMetadata,
                            SpecificOptions options,
                            AsyncPagingOptions asyncPagingOptions,
                            int protocolVersion)
        {
            this.consistency = consistency;
            this.values = values;
            this.skipMetadata = skipMetadata;
            this.options = options;
            this.asyncPagingOptions = asyncPagingOptions;
            this.protocolVersion = protocolVersion;
        }

        public ConsistencyLevel getConsistency()
        {
            return consistency;
        }

        public List<ByteBuffer> getValues()
        {
            return values;
        }

        public boolean skipMetadata()
        {
            return skipMetadata;
        }

        public int getProtocolVersion()
        {
            return protocolVersion;
        }

        SpecificOptions getSpecificOptions()
        {
            return options;
        }

        public AsyncPagingOptions getAsyncPagingOptions() { return asyncPagingOptions; }
    }

    static class QueryOptionsWrapper extends QueryOptions
    {
        protected final QueryOptions wrapped;

        QueryOptionsWrapper(QueryOptions wrapped)
        {
            this.wrapped = wrapped;
        }

        public List<ByteBuffer> getValues()
        {
            return this.wrapped.getValues();
        }

        public ConsistencyLevel getConsistency()
        {
            return wrapped.getConsistency();
        }

        public boolean skipMetadata()
        {
            return wrapped.skipMetadata();
        }

        public int getProtocolVersion()
        {
            return wrapped.getProtocolVersion();
        }

        SpecificOptions getSpecificOptions()
        {
            return wrapped.getSpecificOptions();
        }

        public AsyncPagingOptions getAsyncPagingOptions()
        {
            return wrapped.getAsyncPagingOptions();
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            wrapped.prepare(specs);
            return this;
        }
    }

    /**
     * <code>QueryOptions</code> decorator that provides access to the column specifications.
     */
    static class OptionsWithColumnSpecifications extends QueryOptionsWrapper
    {
        private final ImmutableList<ColumnSpecification> columnSpecs;

        OptionsWithColumnSpecifications(QueryOptions wrapped, List<ColumnSpecification> columnSpecs)
        {
            super(wrapped);
            this.columnSpecs = ImmutableList.copyOf(columnSpecs);
        }

        @Override
        public boolean hasColumnSpecifications()
        {
            return true;
        }

        @Override
        public ImmutableList<ColumnSpecification> getColumnSpecifications()
        {
            return columnSpecs;
        }
    }

    static class OptionsWithNames extends QueryOptionsWrapper
    {
        private final List<String> names;
        private List<ByteBuffer> orderedValues;

        OptionsWithNames(DefaultQueryOptions wrapped, List<String> names)
        {
            super(wrapped);
            this.names = names;
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            super.prepare(specs);

            orderedValues = new ArrayList<ByteBuffer>(specs.size());
            for (int i = 0; i < specs.size(); i++)
            {
                String name = specs.get(i).name.toString();
                for (int j = 0; j < names.size(); j++)
                {
                    if (name.equals(names.get(j)))
                    {
                        orderedValues.add(wrapped.getValues().get(j));
                        break;
                    }
                }
            }
            return this;
        }

        @Override
        public List<ByteBuffer> getValues()
        {
            assert orderedValues != null; // We should have called prepare first!
            return orderedValues;
        }
    }

    // Options that are likely to not be present in most queries
    static class SpecificOptions
    {
        private static final SpecificOptions DEFAULT = new SpecificOptions(-1, null, null, Long.MIN_VALUE);

        private final int pageSize;
        private final PagingState state;
        private final ConsistencyLevel serialConsistency;
        private final long timestamp;

        private SpecificOptions(int pageSize, PagingState state, ConsistencyLevel serialConsistency, long timestamp)
        {
            this.pageSize = pageSize;
            this.state = state;
            this.serialConsistency = serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
            this.timestamp = timestamp;
        }
    }

    // Options that are stored in the extended flags
    static class ExtendedOptions
    {
        private static final ExtendedOptions DEFAULT = new ExtendedOptions(false);

        private final boolean withStreaming;

        private ExtendedOptions(boolean withStreaming)
        {
            this.withStreaming = withStreaming;
        }

        /**
         * @return true if there is at least one member variable that is different from its defalt value.
         */
        private boolean required()
        {
            return withStreaming != false;
        }
    }

    public static class AsyncPagingOptions
    {
        private static final UUID NO_ASYNC_PAGING_UUID = new UUID(0, 0);
        private static final AsyncPagingOptions DEFAULT = new AsyncPagingOptions(NO_ASYNC_PAGING_UUID);

        public enum PageUnit
        {
            BYTES(1),
            ROWS(2);

            private final int id;
            PageUnit(int id)
            {
                this.id = id;
            }


            private static final PageUnit[] pageUnits;
            static
            {
                int maxId = Arrays.stream(PageUnit.values()).map(pu -> pu.id).reduce(0, Math::max);
                pageUnits = new PageUnit[maxId + 1];
                for (PageUnit pu : PageUnit.values())
                {
                    if (pageUnits[pu.id] != null)
                        throw new IllegalStateException("Duplicate page unit id");
                    pageUnits[pu.id] = pu;
                }
            }

            static PageUnit decode(int id)
            {
                if (id >= pageUnits.length || pageUnits[id] == null)
                    throw new ProtocolException(String.format("Unknown page unit %d", id));
                return pageUnits[id];
            }
        }

        public final UUID uuid;
        public final int pageSize;
        public final PageUnit pageUnit;
        public final int maxPages;
        public final int maxPagesPerSecond;

        private AsyncPagingOptions(UUID uuid)
        {
            this(uuid, 0, PageUnit.BYTES, 0, 0);
        }

        private AsyncPagingOptions(UUID uuid, int pageSize, PageUnit pageUnit, int maxPages, int maxPagesPerSecond)
        {
            this.uuid = uuid;
            this.pageSize = pageSize;
            this.pageUnit = pageUnit;
            this.maxPages = maxPages;
            this.maxPagesPerSecond = maxPagesPerSecond;
        }

        public static AsyncPagingOptions decode(ByteBuf body, int version)
        {
            return new AsyncPagingOptions(CBUtil.readUUID(body),
                                          body.readInt(),
                                          PageUnit.decode(body.readInt()),
                                          body.readInt(),
                                          body.readInt());
        }

        public void encode(ByteBuf dest, int version)
        {
            CBUtil.writeUUID(uuid, dest);
            dest.writeInt(pageSize);
            dest.writeInt(pageUnit.id);
            dest.writeInt(maxPages);
            dest.writeInt(maxPagesPerSecond);
        }

        public int encodedSize(int version)
        {
            return CBUtil.sizeOfUUID(uuid) + 16;
        }

        public boolean asyncPagingRequested()
        {
            return !uuid.equals(NO_ASYNC_PAGING_UUID);
        }

        /**
         * Return an estimated size of a buffer that should be able to accommodate the page requested by the user.
         * If the page unit is bytes, then return the page size, if it is in ROWS then return the number of rows
         * multiplied by the average row size.
         *
         * @param rowSize - the average row size in bytes
         * @return the page buffer size in bytes
         */
        public int bufferSize(int rowSize)
        {
            if (pageUnit == PageUnit.BYTES)
                return pageSize;
            else // ROWS
                return pageSize * rowSize;
        }

        /**
         * Return true if we are ready to send a page. If the page unit is rows, simply compare the number
         * of rows with the page size, if it is in bytes see if there is less than avg row size left in
         * the page.
         *
         * @param numRows - the number of rows written so far
         * @param size - the size written so far in bytes
         * @param rowSize - the average row size in bytes
         * @return - true if the page can be considered completed
         */
        public boolean completed(int numRows, int size, int rowSize)
        {
            if (pageUnit == PageUnit.ROWS)
                return numRows >= pageSize;
            else
                return (pageSize - size) <= rowSize;
        }

        /**
         * Return an estimate of the number of rows in a page. If the page unit is rows, simply return the page
         * size. If it is in bytes, divide by page size by the average row size.
         *
         * @param rowSize - the average row size in bytes
         * @return - an estimated number of rows in the page.
         */
        public int estimatedRows(int rowSize)
        {
            if (pageUnit == PageUnit.ROWS)
                return pageSize;
            else
                return (int)Math.ceil(pageSize / rowSize);

        }
    }

    private static class Codec implements CBCodec<QueryOptions>
    {
        private static <E extends Enum<E>> EnumSet<E> deserializeFlags(int flags, E[] values, EnumSet<E> set)
        {
            for (int n = 0; n < values.length; n++)
            {
                if ((flags & (1 << n)) != 0)
                    set.add(values[n]);
            }
            return set;
        }

        public static <E extends Enum<E>> int serializeFlags(EnumSet<E> flags)
        {
            int i = 0;
            for (E flag : flags)
                i |= 1 << flag.ordinal();
            return i;
        }

        private enum Flag
        {
            // The order of that enum matters!!
            VALUES,
            SKIP_METADATA,
            PAGE_SIZE,
            PAGING_STATE,
            SERIAL_CONSISTENCY,
            TIMESTAMP,
            NAMES_FOR_VALUES,
            EXTENDED_FLAGS;

            private static final Flag[] ALL_VALUES = values();

            public static EnumSet<Flag> deserialize(int flags)
            {
                return deserializeFlags(flags, ALL_VALUES, EnumSet.noneOf(Flag.class));
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                return serializeFlags(flags);
            }
        }

        private enum ExtendedFlag
        {
            // The order of this enum matters!!
            WITH_ASYNC_PAGING;

            private static final ExtendedFlag[] ALL_VALUES = values();

            public static EnumSet<ExtendedFlag> deserialize(int flags)
            {
                return deserializeFlags(flags, ALL_VALUES, EnumSet.noneOf(ExtendedFlag.class));
            }

            public static int serialize(EnumSet<ExtendedFlag> flags)
            {
                return serializeFlags(flags);
            }
        }

        public QueryOptions decode(ByteBuf body, int version)
        {
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            EnumSet<Flag> flags = Flag.deserialize((int)body.readByte());
            EnumSet<ExtendedFlag> extendedFlags = flags.contains(Flag.EXTENDED_FLAGS)
                                                  ? ExtendedFlag.deserialize((int)body.readByte())
                                                  : EnumSet.noneOf(ExtendedFlag.class);

            List<ByteBuffer> values = Collections.<ByteBuffer>emptyList();
            List<String> names = null;
            if (flags.contains(Flag.VALUES))
            {
                if (flags.contains(Flag.NAMES_FOR_VALUES))
                {
                    Pair<List<String>, List<ByteBuffer>> namesAndValues = CBUtil.readNameAndValueList(body, version);
                    names = namesAndValues.left;
                    values = namesAndValues.right;
                }
                else
                {
                    values = CBUtil.readValueList(body, version);
                }
            }

            boolean skipMetadata = flags.contains(Flag.SKIP_METADATA);
            flags.remove(Flag.VALUES);
            flags.remove(Flag.SKIP_METADATA);

            SpecificOptions options = SpecificOptions.DEFAULT;
            if (!flags.isEmpty())
            {
                int pageSize = flags.contains(Flag.PAGE_SIZE) ? body.readInt() : -1;
                PagingState pagingState = flags.contains(Flag.PAGING_STATE) ? PagingState.deserialize(CBUtil.readValue(body), version) : null;
                ConsistencyLevel serialConsistency = flags.contains(Flag.SERIAL_CONSISTENCY) ? CBUtil.readConsistencyLevel(body) : ConsistencyLevel.SERIAL;
                long timestamp = Long.MIN_VALUE;
                if (flags.contains(Flag.TIMESTAMP))
                {
                    long ts = body.readLong();
                    if (ts == Long.MIN_VALUE)
                        throw new ProtocolException(String.format("Out of bound timestamp, must be in [%d, %d] (got %d)", Long.MIN_VALUE + 1, Long.MAX_VALUE, ts));
                    timestamp = ts;
                }

                options = new SpecificOptions(pageSize, pagingState, serialConsistency, timestamp);
            }

            AsyncPagingOptions asyncPagingOptions = extendedFlags.contains(ExtendedFlag.WITH_ASYNC_PAGING)
                                                    ? AsyncPagingOptions.decode(body, version)
                                                    : AsyncPagingOptions.DEFAULT;

            DefaultQueryOptions opts = new DefaultQueryOptions(consistency, values, skipMetadata, options, asyncPagingOptions, version);
            return names == null ? opts : new OptionsWithNames(opts, names);
        }

        public void encode(QueryOptions options, ByteBuf dest, int version)
        {
            CBUtil.writeConsistencyLevel(options.getConsistency(), dest);

            EnumSet<Flag> flags = gatherFlags(options);
            dest.writeByte((byte) Flag.serialize(flags));

            EnumSet<ExtendedFlag> extendedFlags = gatherExtendedFlags(options);
            if (flags.contains(Flag.EXTENDED_FLAGS))
                dest.writeByte((byte) ExtendedFlag.serialize(extendedFlags));

            if (flags.contains(Flag.VALUES))
                CBUtil.writeValueList(options.getValues(), dest);
            if (flags.contains(Flag.PAGE_SIZE))
                dest.writeInt(options.getPageSize());
            if (flags.contains(Flag.PAGING_STATE))
                CBUtil.writeValue(options.getPagingState().serialize(version), dest);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(options.getSerialConsistency(), dest);
            if (flags.contains(Flag.TIMESTAMP))
                dest.writeLong(options.getSpecificOptions().timestamp);

            if (extendedFlags.contains(ExtendedFlag.WITH_ASYNC_PAGING))
                options.getAsyncPagingOptions().encode(dest, version);

            // Note that we don't really have to bother with NAMES_FOR_VALUES server side,
            // and in fact we never really encode QueryOptions, only decode them, so we
            // don't bother.
        }

        public int encodedSize(QueryOptions options, int version)
        {
            int size = 0;

            size += CBUtil.sizeOfConsistencyLevel(options.getConsistency());

            EnumSet<Flag> flags = gatherFlags(options);
            size += 1;

            EnumSet<ExtendedFlag> extendedFlags = gatherExtendedFlags(options);
            if (flags.contains(Flag.EXTENDED_FLAGS))
                size += 1;

            if (flags.contains(Flag.VALUES))
                size += CBUtil.sizeOfValueList(options.getValues());
            if (flags.contains(Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(options.getPagingState().serializedSize(version));
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(options.getSerialConsistency());
            if (flags.contains(Flag.TIMESTAMP))
                size += 8;

            if (extendedFlags.contains(ExtendedFlag.WITH_ASYNC_PAGING))
                size += options.getAsyncPagingOptions().encodedSize(version);

            return size;
        }

        private EnumSet<Flag> gatherFlags(QueryOptions options)
        {
            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (options.getValues().size() > 0)
                flags.add(Flag.VALUES);
            if (options.skipMetadata())
                flags.add(Flag.SKIP_METADATA);
            if (options.getPageSize() >= 0)
                flags.add(Flag.PAGE_SIZE);
            if (options.getPagingState() != null)
                flags.add(Flag.PAGING_STATE);
            if (options.getSerialConsistency() != ConsistencyLevel.SERIAL)
                flags.add(Flag.SERIAL_CONSISTENCY);
            if (options.getSpecificOptions().timestamp != Long.MIN_VALUE)
                flags.add(Flag.TIMESTAMP);
            if (options.getAsyncPagingOptions().asyncPagingRequested())
                flags.add(Flag.EXTENDED_FLAGS);
            return flags;
        }

        private EnumSet<ExtendedFlag> gatherExtendedFlags(QueryOptions options)
        {
            EnumSet<ExtendedFlag> flags = EnumSet.noneOf(ExtendedFlag.class);
            if (options.getAsyncPagingOptions().asyncPagingRequested())
                flags.add(ExtendedFlag.WITH_ASYNC_PAGING);
            return flags;
        }
    }
}
