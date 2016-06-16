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
import org.apache.cassandra.transport.Server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AsyncPagingTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPagingTest.class);

    @BeforeClass
    public static void startup()
    {
        requireNetwork(false);
    }

    @Test
    public void testSingleClusteringRow() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(1024)
                                                     .checkRows(true)
                                                     .build())
        {
            helper.readEntireTableWithAsyncPaging(1, 11000000, AsyncPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.readEntireTableWithAsyncPaging(1, 11000, AsyncPagingOptions.PageUnit.BYTES); // should fit 10 rows
            helper.readEntireTableWithAsyncPaging(1, 110000, AsyncPagingOptions.PageUnit.BYTES); // should fit 100 rows
            helper.readEntireTableWithAsyncPaging(1, 1100, AsyncPagingOptions.PageUnit.BYTES); // should fit 1 row
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.readEntireTableWithAsyncPaging(1, 2000, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 1000, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 33, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void testMultipleClusteringRows() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(10)
                                                     .numClusterings(100)
                                                     .partitionSize(1024)
                                                     .checkRows(true)
                                                     .build())
        {
            helper.readEntireTableWithAsyncPaging(1, 11000000, AsyncPagingOptions.PageUnit.BYTES); // should fit all rows
            helper.readEntireTableWithAsyncPaging(1, 1100, AsyncPagingOptions.PageUnit.BYTES); // should fit 30 row
            helper.readEntireTableWithAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.BYTES); // should fit less than 1 row

            helper.readEntireTableWithAsyncPaging(1, 10000, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 10, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 1, AsyncPagingOptions.PageUnit.ROWS);
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
            helper.readEntireTableWithAsyncPaging(1, 102400, AsyncPagingOptions.PageUnit.BYTES); // 1KB * 100 rows
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
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
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
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
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
            long durationMillis = helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
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
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
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
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
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
            helper.readEntireTableWithAsyncPaging(1, 500, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 100, AsyncPagingOptions.PageUnit.ROWS);
            helper.readEntireTableWithAsyncPaging(1, 43, AsyncPagingOptions.PageUnit.ROWS);
        }
    }

    @Test
    public void selectEntireTable1KB() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(1024)
                                                     .build())
        {

            //warmup
            helper.readEntireTablePageByPage(50, 100);
            helper.readEntireTableWithAsyncPaging(50, 104800, AsyncPagingOptions.PageUnit.BYTES); // row size * 100 rows

            logger.info("Total time reading 1KB table with async paging: {} milliseconds",
                        helper.readEntireTableWithAsyncPaging(500, 104800, AsyncPagingOptions.PageUnit.BYTES)); // row size * 100 rows

            logger.info("Total time reading 1KB table page by page: {} milliseconds",
                        helper.readEntireTablePageByPage(500, 100));
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
            //helper.readEntireTablePageByPage(50, 100);
            //helper.readEntireTableWithAsyncPaging(50, 102400, AsyncPagingOptions.PageUnit.BYTES); // 1KB * 100 rows

            logger.info("Total time reading 10KB table with async paging: {} milliseconds",
                        helper.readEntireTableWithAsyncPaging(1000, 102400, AsyncPagingOptions.PageUnit.BYTES)); // 1KB * 100 rows

            //logger.info("Total time reading 10KB table page by page: {} milliseconds",
            //            helper.readEntireTablePageByPage(500, 100));
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
            helper.readEntireTablePageByPage(50, 100);
            helper.readEntireTableWithAsyncPaging(50, 102400, AsyncPagingOptions.PageUnit.BYTES);  // 1KB * 100 rows

            logger.info("Total time reading 10KB table with async paging: {} milliseconds",
                        helper.readEntireTableWithAsyncPaging(500, 102400, AsyncPagingOptions.PageUnit.BYTES)); // 1KB * 100 rows

            logger.info("Total time reading 10KB table page by page: {} milliseconds",
                        helper.readEntireTablePageByPage(500, 100));
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
            helper.readEntireTablePageByPage(50, 5000);
            helper.readEntireTableWithAsyncPaging(50, 5000, AsyncPagingOptions.PageUnit.ROWS);

            logger.info("Total time reading 100B table with async paging: {} milliseconds",
                        helper.readEntireTableWithAsyncPaging(100, 5000, AsyncPagingOptions.PageUnit.ROWS));

            logger.info("Total time reading 100B table page by page: {} milliseconds",
                        helper.readEntireTablePageByPage(100, 5000));
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

    private static class TestBuilder
    {
        private final CQLTester tester;
        private int numPartitions;
        private int numClusterings;
        private int partitionSize;
        private int numClientThreads;
        private int clientPauseMillis;
        private boolean checkRows;
        private int maxRows;
        private int maxPages;
        private int maxPagesPerSecond;
        private int cancelAfter;

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
        private final int protocolVersion;
        private final int numPartitions;
        private final int numClusterings;
        private final int partitionSize;
        private final int numClientThreads;
        private final int clientPauseMillis;
        private final boolean checkRows;
        private final int maxRows;
        private final int maxPages;
        private final int maxPagesPerSecond;
        private final int cancelAfter;

        private Object[][] rows;

        TestHelper(TestBuilder builder)
        {
            this.tester = builder.tester;
            this.protocolVersion = Server.CURRENT_VERSION;
            this.numPartitions = builder.numPartitions;
            this.numClusterings = builder.numClusterings;
            this.partitionSize = builder.partitionSize;
            this.numClientThreads = builder.numClientThreads;
            this.clientPauseMillis = builder.clientPauseMillis;
            this.checkRows = builder.checkRows;
            this.maxRows = builder.maxRows;
            this.maxPages = builder.maxPages;
            this.maxPagesPerSecond = builder.maxPagesPerSecond;
            this.cancelAfter = builder.cancelAfter;
        }

        private void createTable() throws Throwable
        {
            CQLTester.initClientCluster(protocolVersion, new CustomNettyOptions(numClientThreads));
            tester.createTable("CREATE TABLE %s (k int, c int, val1 TEXT, val2 TEXT, PRIMARY KEY(k, c))");

            rows = new Object[numPartitions * numClusterings][];

            int valueSize = partitionSize / numClusterings / 2;
            String text1 = generateText(valueSize);
            String text2 = generateText(valueSize);

            for (int i = 0; i < numPartitions; i++)
            {
                for (int j = 0; j < numClusterings; j++)
                {
                    tester.execute("INSERT INTO %s (k, c, val1, val2) VALUES (?, ?, ?, ?)", i, j, text1, text2);
                    rows[i * numClusterings + j] = row(i, j, text1, text2);
                }
            }

            logger.info("Finished writing.");

            tester.flush();
            logger.info("Finished flushing.");

            tester.compact();
            logger.info("Finished compacting.");
        }

        private String generateText(int size)
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

        private long readEntireTablePageByPage(int numTrials, int pageSizeRows) throws Throwable
        {
            Session session = tester.sessionNet(protocolVersion);
            PreparedStatement prepared = session.prepare(tester.formatQuery("SELECT k, c, val1, val2 FROM %s"));
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
                    if (!resultSet.isFullyFetched()) // the best we can do here is start fetching before processing
                        resultFuture = resultSet.fetchMoreResults(); // this batch of results
                    else
                        resultFuture = null;

                    maybePauseClient();
                    checker.checkPage(resultSet);
                }

                checker.checkAll();
            }

            return (System.nanoTime() - start) / (1000000 * numTrials);

        }

        private long readEntireTableWithAsyncPaging(int numTrials, int pageSize, AsyncPagingOptions.PageUnit pageUnit) throws Throwable
        {
            long start = System.nanoTime();

            String query = "SELECT k, c, val1, val2 FROM %s";
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
            private final List<Object[]> rowsReceived;
            private int numRowsReceived ;
            private int numPagesReceived;
            private ResultSet lastPage;

            CheckResultSet(int pageSize, AsyncPagingOptions.PageUnit pageUnit)
            {
                this.pageSize = pageSize;
                this.pageUnit = pageUnit;
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
                logger.debug("Received {} rows", numRows);

                if (pageUnit == AsyncPagingOptions.PageUnit.ROWS && numRows > 0)
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
                    assertEquals(rowsReceived.size(), rows.length);
                    Collections.sort(rowsReceived, RowComparator);
                    for (int i = 0; i < rows.length; i++)
                    {
                        assertEquals(rows[i].length, rowsReceived.get(i).length);
                        for (int j = 0; j < rows[i].length; j++)
                            assertEquals(rows[i][j], rowsReceived.get(i)[j]);
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
