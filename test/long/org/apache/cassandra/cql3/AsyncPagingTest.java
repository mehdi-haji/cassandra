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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AsyncPagingOptions;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetIterator;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.shaded.netty.channel.EventLoopGroup;
import com.datastax.shaded.netty.channel.nio.NioEventLoopGroup;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.transport.Server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AsyncPagingTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPagingTest.class);
    private static final Random randomGenerator = new Random();

    @BeforeClass
    public static void startup()
    {
        requireNetwork(false);

        long seed = System.nanoTime();
        logger.info("Using seed {}", seed);
        randomGenerator.setSeed(seed);
    }

    @Test
    public void testSingleClusteringRow() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(1024)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 11000000, AsyncPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testAsyncPaging(1, 11000, AsyncPagingOptions.PageUnit.BYTES); // should fit 10 rows
            helper.testAsyncPaging(1, 110000, AsyncPagingOptions.PageUnit.BYTES); // should fit 100 rows
            helper.testAsyncPaging(1, 1100, AsyncPagingOptions.PageUnit.BYTES); // should fit 1 row
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testAsyncPaging(1, 2000, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 1000, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 33, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testMultipleClusteringRows() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 110000000, AsyncPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testAsyncPaging(1, 11000, AsyncPagingOptions.PageUnit.BYTES); // should fit 30 row
            helper.testAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testAsyncPaging(1, 10000, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 1, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testMultipleClusteringRowsWithCompression() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize, true))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 110000000, AsyncPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testAsyncPaging(1, 11000, AsyncPagingOptions.PageUnit.BYTES); // should fit 30 row
            helper.testAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testAsyncPaging(1, 10000, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 1, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testMultipleClusteringRowsWithVariableSize() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new VariableSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 110000000, AsyncPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.testAsyncPaging(1, 11000, AsyncPagingOptions.PageUnit.BYTES); // should fit 30 row
            helper.testAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.testAsyncPaging(1, 10000, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 1, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    /**
     * Test page sizes that are close to the maximum page size allowed.
     */
    @Test
    public void testLargePages() throws Throwable
    {
        int old = DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(4); // the max page size is half this value

        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(100000)
                                                     .schemaSupplier(b -> new VariableSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 4 * 1024 * 1024, AsyncPagingOptions.PageUnit.BYTES); // bigger than max max
            helper.testAsyncPaging(1, 2 * 1024 * 1024, AsyncPagingOptions.PageUnit.BYTES); // max
            helper.testAsyncPaging(1, (2 * 1024 * 1024) - 5000, AsyncPagingOptions.PageUnit.BYTES); // just smaller than max
        }
        finally
        {
            DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(old);
        }
    }

    /**
     * Test page sizes that are close to the maximum page size allowed, with rows that are bigger than the max page size.
     */
    @Test
    public void testLargePagesEvenLargerRows() throws Throwable
    {
        int old = DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(3); // the max page size is half this value

        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(1)
                                                     .partitionSize(2 * 1024 * 1024) //max mutation size is 2.5
                                                     .schemaSupplier(b -> new VariableSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .checkNumberOfRowsInPage(false) // OK to receive fewer rows in page if page size is bigger than max
                                                     .build())
        {
            helper.testAsyncPaging(1, 3 * 1024 * 1024, AsyncPagingOptions.PageUnit.BYTES);
            helper.testAsyncPaging(1, 2 * 1024 * 1024, AsyncPagingOptions.PageUnit.BYTES);
            helper.testAsyncPaging(1, 1 * 1024 * 1024, AsyncPagingOptions.PageUnit.BYTES);

            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 1, AsyncPagingOptions.PageUnit.ROWS);
        }
        finally
        {
            DatabaseDescriptor.setNativeTransportMaxFrameSizeInMb(old);
        }
    }

    /**
     * Make 10% of the rows larger than other rows.
     */
    @Test
    public void testAbnormallyLargeRows() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(10000)
                                                     .schemaSupplier(b -> new AbonormallyLargeRowsSchema(b.numPartitions, b.numClusterings, b.partitionSize, 0.1))
                                                     .checkRows(true)
                                                     .build())
        {

            helper.testAsyncPaging(1, 10000, AsyncPagingOptions.PageUnit.BYTES);
            helper.testAsyncPaging(1, 20000, AsyncPagingOptions.PageUnit.BYTES);
            helper.testAsyncPaging(1, 50000, AsyncPagingOptions.PageUnit.BYTES);
            helper.testAsyncPaging(1, 100000, AsyncPagingOptions.PageUnit.BYTES);

            helper.testAsyncPaging(1, 50, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testSelectSinglePartition() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new SelectInitialPartitionsSchema(b.numPartitions, b.numClusterings, b.partitionSize, 1))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 10000, AsyncPagingOptions.PageUnit.BYTES);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testSelectOnlyFirstTenPartitions() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new SelectInitialPartitionsSchema(b.numPartitions, b.numClusterings, b.partitionSize, 10))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 10000, AsyncPagingOptions.PageUnit.BYTES);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    /**
     * Test interrupting async paging after N pages, and then resuming again.
     */
    @Test
    public void testResumeWithAsyncPaging() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testResumeWithAsyncPaging(50, AsyncPagingOptions.PageUnit.ROWS, 50);
            helper.testResumeWithAsyncPaging(100, AsyncPagingOptions.PageUnit.ROWS, 10);
            helper.testResumeWithAsyncPaging(1000, AsyncPagingOptions.PageUnit.ROWS, 5);

            helper.testResumeWithAsyncPaging(5000, AsyncPagingOptions.PageUnit.BYTES, 15);
        }
    }

    /**
     * Test async paging for the first N pages and then completing with page-by-page.
     */
    @Test
    public void testResumeWithLegacy() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(1000)
                                                     .schemaSupplier(b -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize))
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testResumeWithLegacy(50, 50);
            helper.testResumeWithLegacy(100, 10);
            helper.testResumeWithLegacy(1000, 5);
        }
    }

    /**
     * Simulates a slow client with only 1 NIO thread and a long pause on the main thread.
     * Use a small page size compared to the number of rows, so that eventually messages should start to queue up
     * and the NIO thread should become blocked, see StreamingRequestHandlerCallback.onData() in the driver.
     * @throws Throwable
     */
    @Test
    public void testSlowClient() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .numClientThreads(1)
                                                     .clientPauseMillis(10)
                                                     .checkRows(true)
                                                     .build())
        {
            helper.testAsyncPaging(1, 102400, AsyncPagingOptions.PageUnit.BYTES); // 1KB * 100 rows
        }
    }

    /** Cancel after 3 pages, we should end up with an incomplete query (paging state != null) */
    @Test
    public void testCancel() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .cancelAfter(3)
                                                     .build())
        {
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    /** Cancel just before the last page, this should trigger a failed cancellation server side. */
    @Test
    public void testCancelLate() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .cancelAfter(99)
                                                     .build())
        {
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    /** Throttle at 2 pages per second, make sure pages are not received too quickly. */
    @Test
    public void testThrottle() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxPagesPerSecond(2)
                                                     .checkRows(true)
                                                     .build())
        {
            long durationMillis = helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            assertTrue(String.format("Finished too quickly (%d millis) for this throttling level", durationMillis),
                       durationMillis >= 45000); // 100-10=90 pages at 2 / second -> 45 seconds at least, we exclude
                                                 // the first 10 pages because of the RateLimiter smoothing factor
        }
    }

    /** Test that if we specify a maximum number of pages then we receive at most this number. */
    @Test
    public void testMaxPages() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxPages(10)
                                                     .build())
        {
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    /** Test that we can receive a single page only. */
    @Test
    public void testSinglePage() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxPages(1)
                                                     .build())
        {
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    /** Test that if we specify a maximum number of rows then we receive at most this number. */
    @Test
    public void testMaxRows() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(100)
                                                     .numClusterings(100)
                                                     .partitionSize(64)
                                                     .maxRows(300)
                                                     .build())
        {
            helper.testAsyncPaging(1, 500, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.testAsyncPaging(1, 43, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void selectEntireTable1KB() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(1024)
                                                     .checkRows(true)
                                                     .build())
        {

            //warmup
            helper.testLegacyPaging(50, 100);
            helper.testAsyncPaging(50, 104800, AsyncPagingOptions.PageUnit.BYTES); // row size * 100 rows

            logger.info("Total time reading 1KB table with async paging: {} milliseconds",
                        helper.testAsyncPaging(500, 104800, AsyncPagingOptions.PageUnit.BYTES)); // row size * 100 rows

            logger.info("Total time reading 1KB table page by page: {} milliseconds",
                        helper.testLegacyPaging(500, 100));
        }
    }

   // @Ignore("Long test intended to be run manually for profiling and benchmarking")
    @Test
    public void selectEntireTable10KB() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(10*1024)
                                                     .build())
        {
            //warmup
            helper.testLegacyPaging(50, 100);
            helper.testAsyncPaging(50, 102400, AsyncPagingOptions.PageUnit.BYTES); // 1KB * 100 rows

            logger.info("Total time reading 10KB table with async paging: {} milliseconds",
                        helper.testAsyncPaging(1000, 102400, AsyncPagingOptions.PageUnit.BYTES)); // 1KB * 100 rows

            logger.info("Total time reading 10KB table page by page: {} milliseconds",
                        helper.testLegacyPaging(500, 100));
        }
    }


    @Ignore("Long test intended to be run manually for profiling and benchmarking")
    public void selectEntireTable64KB() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(64*1024)
                                                     .build())
        {
            //warmup
            helper.testLegacyPaging(50, 100);
            helper.testAsyncPaging(50, 102400, AsyncPagingOptions.PageUnit.BYTES);  // 1KB * 100 rows

            logger.info("Total time reading 10KB table with async paging: {} milliseconds",
                        helper.testAsyncPaging(500, 102400, AsyncPagingOptions.PageUnit.BYTES)); // 1KB * 100 rows

            logger.info("Total time reading 10KB table page by page: {} milliseconds",
                        helper.testLegacyPaging(500, 100));
        }
    }

    @Ignore("Long test intended to be run manually for profiling and benchmarking")
    public void selectEntireTable100B() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(100)
                                                     .partitionSize(100)
                                                     .build())
        {
            //warmup
            helper.testLegacyPaging(50, 5000);
            helper.testAsyncPaging(50, 5000, AsyncPagingOptions.PageUnit.ROWS);

            logger.info("Total time reading 100B table with async paging: {} milliseconds",
                        helper.testAsyncPaging(100, 5000, AsyncPagingOptions.PageUnit.ROWS));

            logger.info("Total time reading 100B table page by page: {} milliseconds",
                        helper.testLegacyPaging(100, 5000));
        }
    }

    private static class CustomNettyOptions extends NettyOptions
    {
        private final int numThreads; // zero means use the netty default value

        CustomNettyOptions(int numThreads)
        {
            this.numThreads = numThreads;
        }

        public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory)
        {
            // the driver should use NIO anyway when Netty is shaded
            return new NioEventLoopGroup(numThreads, threadFactory);
        }
    }

    private abstract static class TestSchema
    {
        final int numPartitions;
        final int numClusterings;
        final int partitionSize;

        TestSchema(int numPartitions, int numClusterings, int partitionSize)
        {
            this.numPartitions = numPartitions;
            this.numClusterings = numClusterings;
            this.partitionSize = partitionSize;
        }

        static String generateText(int size)
        {
            Random rnd = new Random();
            char[] chars = new char[size];

            for (int i = 0; i < size; )
                for (long v = rnd.nextLong(),
                     n = Math.min(size - i, Long.SIZE / Byte.SIZE);
                     n-- > 0; v >>= Byte.SIZE)
                    chars[i++] = (char) (((v & 127) + 32) & 127);
            return new String(chars, 0, size);
        }

        /** Create the rows that will populate the table */
        abstract void createRows();

        /** Return the text of the create statement. */
        abstract String createStatement();

        /** Return the text of the insert statement. */
        abstract String insertStatement();

        /** Return an array of rows that should be inserted in the table. */
        abstract Object[][] insertRows();

        /** Return the text of the select statement. */
        abstract String selectStatement();

        /** Return an array of rows that the select statement should return. */
        abstract Object[][] selectRows();
    }

    /**
     * A schema with one partition key, one clustering key two text values of
     * identical size and a static column.
     */
    private static class FixedSizeSchema extends TestSchema
    {
        final Object[][] rows;
        final boolean compression;

        FixedSizeSchema(int numPartitions, int numClusterings, int partitionSize)
        {
            this(numPartitions, numClusterings, partitionSize, false);
        }

        FixedSizeSchema(int numPartitions, int numClusterings, int partitionSize, boolean compression)
        {
            super(numPartitions, numClusterings, partitionSize);
            this.rows = new Object[numPartitions * numClusterings][];
            this.compression = compression;
        }

        void createRows()
        {
            // These are CQL sizes and at the moment we duplicate partition and static values in each CQL row
            // 12 is the size of the 3 integers (pk, ck, static val)
            int rowSize = partitionSize / numClusterings;
            int textSize = Math.max(1, (rowSize - 12) / 2);
            for (int i = 0; i < numPartitions; i++)
            {
                for (int j = 0; j < numClusterings; j++)
                {
                    String text1 = generateText(textSize);
                    String text2 = generateText(textSize);
                    rows[i * numClusterings + j] = row(i, j, text1, text2, i);
                }
            }
        }

        public String createStatement()
        {
            String ret = "CREATE TABLE %s (k INT, c INT, val1 TEXT, val2 TEXT, s INT STATIC, PRIMARY KEY(k, c))";
            if (!compression)
                ret += " WITH compression = {'sstable_compression' : ''}";
            return ret;
        }

        public String insertStatement()
        {
            return "INSERT INTO %s (k, c, val1, val2, s) VALUES (?, ?, ?, ?, ?)";
        }

        public Object[][] insertRows()
        {
            return rows;
        }

        public String selectStatement()
        {
            return "SELECT k, c, val1, val2, s FROM %s";
        }

        public Object[][] selectRows()
        {
            return rows;
        }
    }

    /**
     * This schema is the same as the fixed size schema except that each text value
     * has a random size, making each row of size different size.
     */
    private static class VariableSizeSchema extends FixedSizeSchema
    {
        VariableSizeSchema(int numPartitions, int numClusterings, int partitionSize)
        {
            super(numPartitions, numClusterings, partitionSize);
        }

        @Override
        void createRows()
        {
            int rowSize = partitionSize / numClusterings;
            int textSize = Math.max(1, (rowSize - 12) / 2);
            for (int i = 0; i < numPartitions; i++)
            {
                for (int j = 0; j < numClusterings; j++)
                {
                    String text1 = generateText(1 + randomGenerator.nextInt(textSize));
                    String text2 = generateText(1 + randomGenerator.nextInt(textSize));
                    rows[i * numClusterings + j] = row(i, j, text1, text2, i);
                }
            }
        }
    }

    /**
     * This schema is the same as the fixed size schema except that it introduces rows
     * that are abnormally large.
     */
    private static class AbonormallyLargeRowsSchema extends FixedSizeSchema
    {
        /** The percentage of large rows */
        private final double percentageLargeRows;

        AbonormallyLargeRowsSchema(int numPartitions, int numClusterings, int partitionSize, double percentageLargeRows)
        {
            super(numPartitions, numClusterings, partitionSize);
            this.percentageLargeRows = percentageLargeRows;
        }

        @Override
        void createRows()
        {
            // These are CQL sizes and at the moment we duplicate partition and static values in each CQL row
            // 12 is the size of the 3 integers (pk, ck, static val)
            int rowSize = partitionSize / numClusterings;
            int textSize = Math.max(1, (rowSize - 12) / 2);

            int totRows = numPartitions * numClusterings;
            int largeRows = (int)(totRows * percentageLargeRows);
            int[] largeIndexes = randomGenerator.ints(0, totRows).distinct().limit(largeRows).toArray();
            Arrays.sort(largeIndexes);
            int currentRow = 0;
            int nextLargeRow = 0;

            for (int i = 0; i < numPartitions; i++)
            {
                for (int j = 0; j < numClusterings; j++)
                {
                    int size;
                    if (nextLargeRow < largeIndexes.length && currentRow == largeIndexes[nextLargeRow])
                    {   // make this row up to 50 times bigger (2 text values up to 25 times bigger)
                        size = textSize * (1 + randomGenerator.nextInt(25));
                        nextLargeRow++;
                    }
                    else
                    {
                        size = textSize;
                    }
                    String text1 = generateText(size);
                    String text2 = generateText(size);
                    rows[i * numClusterings + j] = row(i, j, text1, text2, i);
                    currentRow++;
                }
            }
        }
    }

    /**
     * This schema is the same as the fixed size schema except that the select
     * statement will only select the rows in the first N partitions. This
     * schema is very important for testing because at the moment we do not
     * optimize single partition queries or multi-partition queries with a SELECT IN,
     * which means this is the only way we can test the legacy path with unit tests,
     * otherwise we need to use dtests with CL > 1.
     * See {@link SelectStatement.ExecutorBuilder#isLocalRangeQuery()}.
     */
    private static class SelectInitialPartitionsSchema extends FixedSizeSchema
    {
        private final int numSelectPartitions;

        SelectInitialPartitionsSchema(int numPartitions, int numClusterings, int partitionSize, int numSelectPartitions)
        {
            super(numPartitions, numClusterings, partitionSize);
            this.numSelectPartitions = numSelectPartitions;
        }

        public String selectStatement()
        {
            if (numSelectPartitions == 1)
                return "SELECT k, c, val1, val2, s FROM %s WHERE k = 0";

            return "SELECT k, c, val1, val2, s FROM %s WHERE k in ("
                   + IntStream.range(0, numSelectPartitions).mapToObj(Integer::toString).collect(Collectors.joining(", "))
                   + ')';
        }

        public Object[][] selectRows()
        {
            return Arrays.copyOfRange(rows, 0, numSelectPartitions * numClusterings);
        }
    }

    private static class TestBuilder
    {
        private final CQLTester tester;
        private int numPartitions;
        private int numClusterings;
        private int partitionSize;
        private int numClientThreads;
        private int clientPauseMillis;
        private boolean checkRows;
        private boolean checkNumberOfRowsInPage = true;
        private int maxRows;
        private int maxPages;
        private int maxPagesPerSecond;
        private int cancelAfter;
        private Function<TestBuilder, TestSchema> schemaSupplier;

        TestBuilder(CQLTester tester)
        {
            this.tester = tester;
        }

        TestBuilder numPartitions(int numPartitions)
        {
            this.numPartitions = numPartitions;
            return this;
        }

        TestBuilder numClusterings(int numClusterings)
        {
            this.numClusterings = numClusterings;
            return this;
        }

        TestBuilder partitionSize(int partitionSize)
        {
            this.partitionSize = partitionSize;
            return this;
        }

        TestBuilder numClientThreads(int numClientThreads)
        {
            this.numClientThreads = numClientThreads;
            return this;
        }

        TestBuilder clientPauseMillis(int clientPauseMillis)
        {
            this.clientPauseMillis = clientPauseMillis;
            return this;
        }

        TestBuilder checkRows(boolean checkRows)
        {
            this.checkRows = checkRows;
            return this;
        }

        TestBuilder checkNumberOfRowsInPage(boolean checkNumberOfRowsInPage)
        {
            this.checkNumberOfRowsInPage = checkNumberOfRowsInPage;
            return this;
        }

        TestBuilder maxRows(int maxRows)
        {
            this.maxRows = maxRows;
            return this;
        }

        TestBuilder maxPages(int maxPages)
        {
            this.maxPages = maxPages;
            return this;
        }

        TestBuilder maxPagesPerSecond(int maxPagesPerSecond)
        {
            this.maxPagesPerSecond = maxPagesPerSecond;
            return this;
        }

        TestBuilder cancelAfter(int cancelAfter)
        {
            this.cancelAfter = cancelAfter;
            return this;
        }
        TestBuilder schemaSupplier(Function<TestBuilder, TestSchema> schemaSupplier)
        {
            this.schemaSupplier = schemaSupplier;
            return this;
        }

        TestSchema buildSchema()
        {
            if (this.schemaSupplier == null)
                this.schemaSupplier = (b) -> new FixedSizeSchema(b.numPartitions, b.numClusterings, b.partitionSize);

            TestSchema ret = schemaSupplier.apply(this);
            ret.createRows();
            return ret;
        }

        public TestHelper build() throws Throwable
        {
            TestHelper ret = new TestHelper(this);
            ret.createTable();
            return ret;
        }
    }

    private static class TestHelper implements AutoCloseable
    {
        private final CQLTester tester;
        private final TestSchema schema;
        private final int protocolVersion;
        private final int numClientThreads;
        private final int clientPauseMillis;
        private final boolean checkRows;
        private final boolean checkNumberOfRowsInPage;
        private final int maxRows;
        private final int maxPages;
        private final int maxPagesPerSecond;
        private final int cancelAfter;

        TestHelper(TestBuilder builder)
        {
            this.tester = builder.tester;
            this.schema = builder.buildSchema();
            this.protocolVersion = Server.CURRENT_VERSION;
            this.numClientThreads = builder.numClientThreads;
            this.clientPauseMillis = builder.clientPauseMillis;
            this.checkRows = builder.checkRows;
            this.checkNumberOfRowsInPage = builder.checkNumberOfRowsInPage;
            this.maxRows = builder.maxRows;
            this.maxPages = builder.maxPages;
            this.maxPagesPerSecond = builder.maxPagesPerSecond;
            this.cancelAfter = builder.cancelAfter;
        }

        private void createTable() throws Throwable
        {
            CQLTester.initClientCluster(protocolVersion, new CustomNettyOptions(numClientThreads));
            tester.createTable(schema.createStatement());

            for (Object[] row : schema.insertRows())
                tester.execute(schema.insertStatement(), row);
            logger.info("Finished writing.");

            tester.flush();
            logger.info("Finished flushing.");

            tester.compact();
            logger.info("Finished compacting.");
        }

        private long testLegacyPaging(int numTrials, int pageSizeRows) throws Throwable
        {
            Session session = tester.sessionNet(protocolVersion);
            PreparedStatement prepared = session.prepare(tester.formatQuery(schema.selectStatement()));
            BoundStatement statement = prepared.bind();
            statement.setFetchSize(pageSizeRows);

            long start = System.nanoTime();
            for (int i = 0; i < numTrials; i++)
            {
                ListenableFuture<ResultSet> resultFuture = session.executeAsync(statement);
                final CheckResultSet checker = new CheckResultSet(pageSizeRows, AsyncPagingOptions.PageUnit.ROWS);
                while (resultFuture != null)
                {
                    final ResultSet resultSet = resultFuture.get();
                    checker.checkPage(resultSet); // must check before fetching or we may receive too many rows in current page
                    if (!resultSet.isFullyFetched()) // the best we can do here is start fetching before processing
                        resultFuture = resultSet.fetchMoreResults(); // this batch of results
                    else
                        resultFuture = null;

                    maybePauseClient();
                }

                checker.checkAll();
            }

            return (System.nanoTime() - start) / (1000000 * numTrials);

        }

        private long testAsyncPaging(int numTrials, int pageSize, AsyncPagingOptions.PageUnit pageUnit) throws Throwable
        {
            long start = System.nanoTime();

            String query = schema.selectStatement();
            if (maxRows > 0)
                query += String.format(" LIMIT %d", maxRows);

            for (int i = 0; i < numTrials; i++)
            {
                Session session = tester.sessionNet(protocolVersion);
                Statement statement = new SimpleStatement(tester.formatQuery(query));

                final CheckResultSet checker = new CheckResultSet(pageSize, pageUnit);

                try (ResultSetIterator it = session.executeAsync(statement, AsyncPagingOptions.create(pageSize, pageUnit, maxPages, maxPagesPerSecond)))
                {
                    int num = 0;
                    while (it.hasNext())
                    {
                        ResultSet resultSet = it.next();
                        maybePauseClient();
                        checker.checkPage(resultSet);

                        num++;
                        if (cancelAfter > 0 && num >= cancelAfter)
                            break;
                    }
                }
                checker.checkAll();
            }

            return (System.nanoTime() - start) / (1000000 * numTrials);
        }

        /**
         * Read the entire table starting with async paging, interrupting and resuming again.
         *
         * @param pageSize - the page size in the page unit specified
         * @param pageUnit  - the page unit, bytes or rows
         * @param interruptAfter - the number of pages after which we interrupt paging
         * @return the time it took in milliseconds
         */
        private long testResumeWithAsyncPaging(int pageSize, AsyncPagingOptions.PageUnit pageUnit, int interruptAfter) throws Throwable
        {
            long start = System.nanoTime();

            String query = schema.selectStatement();
            if (maxRows > 0)
                query += String.format(" LIMIT %d", maxRows);

            Session session = tester.sessionNet(protocolVersion);
            Statement statement = new SimpleStatement(tester.formatQuery(query));
            statement.setFetchSize(pageSize);

            final CheckResultSet checker = new CheckResultSet(pageSize, pageUnit);
            ResultSet resultSet = null;

            try (ResultSetIterator it = session.executeAsync(statement, AsyncPagingOptions.create(pageSize, pageUnit, maxPages, maxPagesPerSecond)))
            {
                int num = 0;
                while (it.hasNext() && num < interruptAfter)
                {
                    resultSet = it.next();
                    checker.checkPage(resultSet);
                    num++;
                }
            }

            if (resultSet != null && !resultSet.isFullyFetched())
            {
                try (ResultSetIterator it = resultSet.fetchRemainingResults())
                {
                    while (it.hasNext())
                    {
                        resultSet = it.next();
                        checker.checkPage(resultSet);
                    }
                }
            }

            checker.checkAll();

            return (System.nanoTime() - start) / 1000000;
        }

        /**
         * Read the entire table starting with async paging and switching to legacy paging after interruptAfter.
         *
         * @param pageSize - the page size in rows
         * @param interruptAfter - the number of pages after which we interrupt async paging
         * @return the time it took in milliseconds
         */
        private long testResumeWithLegacy(int pageSize, int interruptAfter) throws Throwable
        {
            long start = System.nanoTime();

            String query = schema.selectStatement();
            if (maxRows > 0)
                query += String.format(" LIMIT %d", maxRows);


            Session session = tester.sessionNet(protocolVersion);
            Statement statement = new SimpleStatement(tester.formatQuery(query));
            statement.setFetchSize(pageSize);

            AsyncPagingOptions.PageUnit pageUnit = AsyncPagingOptions.PageUnit.ROWS;
            final CheckResultSet checker = new CheckResultSet(pageSize, pageUnit);
            ResultSet resultSet = null;

            try (ResultSetIterator it = session.executeAsync(statement, AsyncPagingOptions.create(pageSize, pageUnit, maxPages, maxPagesPerSecond)))
            {
                int num = 0;
                while (it.hasNext() && num < interruptAfter)
                {
                    resultSet = it.next();
                    checker.checkPage(resultSet);
                    num++;
                }
            }

            if (resultSet != null && !resultSet.isFullyFetched())
            {
                ListenableFuture<ResultSet> resultFuture = resultSet.fetchMoreResults();
                while (resultFuture != null)
                {
                    resultSet = resultFuture.get();
                    if (!resultSet.isFullyFetched()) // the best we can do here is start fetching before processing
                        resultFuture = resultSet.fetchMoreResults(); // this batch of results
                    else
                        resultFuture = null;

                    checker.checkPage(resultSet);
                }
            }

            checker.checkAll();

            return (System.nanoTime() - start) / 1000000;
        }

        private void maybePauseClient() throws Throwable
        {
            if (clientPauseMillis > 0)
                Thread.sleep(clientPauseMillis);
        }

        //simulates performing some processing with the results
        private class CheckResultSet
        {
            private final int pageSize;
            private final AsyncPagingOptions.PageUnit pageUnit;
            private final Object[][] rows;
            private final List<Object[]> rowsReceived;
            private int numRowsReceived ;
            private int numPagesReceived;
            private ResultSet lastPage;

            CheckResultSet(int pageSize, AsyncPagingOptions.PageUnit pageUnit)
            {
                this.pageSize = pageSize;
                this.pageUnit = pageUnit;
                this.rows = schema.selectRows();
                this.rowsReceived = new ArrayList<>(rows.length);
            }

            Comparator<Object[]> RowComparator = (Comparator<Object[]>) (row1, row2) -> {
                int ret = Integer.compare(row1.length, row2.length);
                if (ret != 0)
                    return ret;

                for (int i = 0; i < row1.length; i++)
                {
                    if (row1[i] instanceof Integer && row2[i] instanceof Integer)
                        ret = Integer.compare((int)row1[i], (int)row2[i]);
                    else if (row1[i] instanceof String && row2[i] instanceof String)
                        ret = ((String)row1[i]).compareTo((String)row2[i]);
                    else
                        ret = Integer.compare(row1[1].hashCode(), row2[1].hashCode());
                    if (ret != 0)
                        return ret;
                }

                return 0;
            };

            private synchronized void checkPage(ResultSet resultSet)
            {
                int numRows = resultSet.getAvailableWithoutFetching();
                logger.trace("Received {} rows", numRows);

                if (checkNumberOfRowsInPage && pageUnit == AsyncPagingOptions.PageUnit.ROWS && numRows > 0)
                {
                    int totRows = maxRows > 0 ? maxRows : rows.length;
                    assertEquals(String.format("PS %d, tot %d, received %d", pageSize, totRows, numPagesReceived),
                                 Math.min(pageSize, totRows - numRowsReceived), numRows);
                }

                if (checkRows)
                {
                    rowsReceived.addAll(Arrays.asList(tester.getRowsNet(protocolVersion, resultSet)));
                }
                else
                {
                    for (int i = 0; i < numRows; i++)
                    {
                        assertTrue(resultSet.iterator().hasNext());
                        assertNotNull(resultSet.iterator().next());
                    }
                }

                numRowsReceived += numRows;
                numPagesReceived += 1;
                lastPage = resultSet;

            }

            private void checkAll()
            {
                if (maxPages > 0)
                { // check that we've received exactly the number of pages requested
                    assertFalse("Cannot check rows if receiving fewer pages", checkRows);
                    assertEquals(maxPages, numPagesReceived);
                    if (numRowsReceived != rows.length)
                        assertFalse(lastPage.isFullyFetched());
                    return;
                }

                if (maxRows > 0)
                {   // check that we've received exactly the number of rows requested
                    assertFalse("Cannot check rows if receiving fewer pages", checkRows);
                    assertEquals(maxRows, numRowsReceived);
                    assertTrue(lastPage.isFullyFetched());
                    return;
                }

                if (cancelAfter > 0)
                {   // check that we haven't received too few pages and that the last page
                    // still has more too fetch (this could become flacky if client is too fast)
                    assertFalse("Cannot check rows if receiving fewer pages", checkRows);
                    logger.info("Received {} pages when cancelling after {} pages", numPagesReceived, cancelAfter);
                    assertTrue(String.format("%d < %d", numPagesReceived, cancelAfter), numPagesReceived >= cancelAfter);
                    assertFalse(lastPage.isFullyFetched());
                    return;
                }

                //otherwise check we've received all table rows
                assertEquals(rows.length, numRowsReceived);

                //check every single row matches if so requested, requires sorting rows
                if (checkRows)
                {
                    assertEquals("Received different number of rows", rowsReceived.size(), rows.length);
                    Collections.sort(rowsReceived, RowComparator);
                    for (int i = 0; i < rows.length; i++)
                    {
                        assertEquals(rows[i].length, rowsReceived.get(i).length);
                        for (int j = 0; j < rows[i].length; j++)
                            assertEquals(String.format("Row %d column %d were different", i, j),
                                         rows[i][j], rowsReceived.get(i)[j]);
                    }
                }
            }
        }

        public void close() throws Exception
        {
            CQLTester.closeClientCluster(protocolVersion);
        }
    }
}
