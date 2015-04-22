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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.primitives.Longs;
import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.junit.Assert.assertEquals;

public class CompactionAwareWriterTest extends CQLTester
{
    private static final String KEYSPACE = "cawt_keyspace";
    private static final String TABLE = "cawt_table";

    private static final int ROW_PER_PARTITION = 10;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // Disabling durable write since we don't care
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes=false");
        schemaChange(String.format("CREATE TABLE %s.%s (k int, v int, PRIMARY KEY (k, v))", KEYSPACE, TABLE));
    }

    @AfterClass
    public static void tearDownClass()
    {
        QueryProcessor.executeInternal("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    private ColumnFamilyStore getColumnFamilyStore()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    @Test
    public void testDefaultCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            int rowCount = 1000;
            cfs.disableAutoCompaction();
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            CompactionAwareWriter writer = new DefaultCompactionWriter(cfs, sstables, sstables, false, OperationType.COMPACTION);
            int rows = compact(cfs, sstables, writer);
            validateData(cfs, rowCount);
            assertEquals(1, cfs.getSSTables().size());
            assertEquals(rowCount, rows);
            assertEquals(beforeSize, cfs.getSSTables().iterator().next().onDiskLength());
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    @Test
    public void testMaxSSTableSizeWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            cfs.disableAutoCompaction();
            int rowCount = 100;
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            int sstableCount = (int)beforeSize/10;
            CompactionAwareWriter writer = new MaxSSTableSizeWriter(cfs, sstables, sstables, sstableCount, 0, false, OperationType.COMPACTION);
            int rows = compact(cfs, sstables, writer);
            validateData(cfs, rowCount);
            assertEquals(10, cfs.getSSTables().size());
            assertEquals(rowCount, rows);
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    @Test
    public void testSplittingSizeTieredCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            cfs.disableAutoCompaction();
            int rowCount = 10000;
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            CompactionAwareWriter writer = new SplittingSizeTieredCompactionWriter(cfs, sstables, sstables, OperationType.COMPACTION, 0);
            int rows = compact(cfs, sstables, writer);
            validateData(cfs, rowCount);
            long expectedSize = beforeSize / 2;
            List<SSTableReader> sortedSSTables = new ArrayList<>(cfs.getSSTables());

            Collections.sort(sortedSSTables, new Comparator<SSTableReader>()
            {
                @Override
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    return Longs.compare(o2.onDiskLength(), o1.onDiskLength());
                }
            });
            for (SSTableReader sstable : sortedSSTables)
            {
                assertEquals(expectedSize, sstable.onDiskLength(), 10000);
                expectedSize /= 2;
            }
            assertEquals(rowCount, rows);
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    @Test
    public void testMajorLeveledCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        try
        {
            cfs.disableAutoCompaction();
            int rowCount = 10000;
            populate(rowCount);
            Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
            long beforeSize = sstables.iterator().next().onDiskLength();
            int sstableCount = (int)beforeSize/100;
            CompactionAwareWriter writer = new MajorLeveledCompactionWriter(cfs, sstables, sstables, sstableCount, false, OperationType.COMPACTION);
            int rows = compact(cfs, sstables, writer);
            validateData(cfs, rowCount);
            assertEquals(100, cfs.getSSTables().size());
            int [] levelCounts = new int[5];
            assertEquals(rowCount, rows);
            for (SSTableReader sstable : cfs.getSSTables())
            {
                levelCounts[sstable.getSSTableLevel()]++;
            }
            assertEquals(0, levelCounts[0]);
            assertEquals(10, levelCounts[1]);
            assertEquals(90, levelCounts[2]);
            for (int i = 3; i < levelCounts.length; i++)
                assertEquals(0, levelCounts[i]);
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    private int compact(ColumnFamilyStore cfs, Set<SSTableReader> sstables, CompactionAwareWriter writer)
    {
        assert sstables.size() == 1;
        int rowsWritten = 0;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
             CompactionIterable ci = new CompactionIterable(OperationType.COMPACTION, scanners.scanners, controller, SSTableFormat.Type.BIG))
        {
            while (ci.hasNext())
            {
                if (writer.append(ci.next()))
                    rowsWritten++;
            }
        }
        Collection<SSTableReader> newSSTables = writer.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, newSSTables, OperationType.COMPACTION);
        return rowsWritten;
    }

    private void populate(int count) throws Throwable
    {
        for (int i = 0; i < count; i++)
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                execute(String.format("INSERT INTO %s.%s(k, v) VALUES (:i, :j)", KEYSPACE, TABLE), i, j);

        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.forceBlockingFlush();
        assert cfs.getSSTables().size() == 1 : cfs.getSSTables();
    }

    private void validateData(ColumnFamilyStore cfs, int rowCount) throws Throwable
    {
        for (int i = 0; i < rowCount; i++)
        {
            Object[][] expected = new Object[ROW_PER_PARTITION][];
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                expected[j] = row(i, j);

            assertRows(execute(String.format("SELECT * FROM %s.%s WHERE k = :i", KEYSPACE, TABLE), i), expected);
        }
    }
}
