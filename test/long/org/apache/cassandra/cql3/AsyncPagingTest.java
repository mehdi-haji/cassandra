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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.BeforeClass;
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
            helper.readEntireTableWithBulkPaging(1, 100);
            helper.readEntireTableWithBulkPaging(1, 27);
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
            helper.readEntireTableWithBulkPaging(1, 100);
            helper.readEntireTableWithBulkPaging(1, 1);
            helper.readEntireTableWithBulkPaging(1, 25);
            helper.readEntireTableWithBulkPaging(1, 150);
            helper.readEntireTableWithBulkPaging(1, 101);
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
                                                     .build())
        {
            helper.readEntireTableWithBulkPaging(1, 10);
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
            helper.readEntireTableWithBulkPaging(50, 102400); // 1KB * 100 rows

            logger.info("Total time reading 1KB table with bulk paging: {} milliseconds",
                        helper.readEntireTableWithBulkPaging(500, 102400)); // 1KB * 100 rows

            logger.info("Total time reading 1KB table page by page: {} milliseconds",
                        helper.readEntireTablePageByPage(500, 100));
        }
    }

    @Test
    public void selectEntireTable10KB() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(10*1024)
                                                     .build())
        {
            //warmup
            helper.readEntireTablePageByPage(50, 100);
            helper.readEntireTableWithBulkPaging(50, 102400); // 1KB * 100 rows

            logger.info("Total time reading 10KB table with bulk paging: {} milliseconds",
                        helper.readEntireTableWithBulkPaging(500, 102400)); // 1KB * 100 rows

            logger.info("Total time reading 10KB table page by page: {} milliseconds",
                        helper.readEntireTablePageByPage(500, 100));
        }
    }


    @Test
    public void selectEntireTable64KB() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(1)
                                                     .partitionSize(64*1024)
                                                     .build())
        {
            //warmup
            helper.readEntireTablePageByPage(50, 100);
            helper.readEntireTableWithBulkPaging(50, 102400);  // 1KB * 100 rows

            logger.info("Total time reading 10KB table with bulk paging: {} milliseconds",
                        helper.readEntireTableWithBulkPaging(500, 102400)); // 1KB * 100 rows

            logger.info("Total time reading 10KB table page by page: {} milliseconds",
                        helper.readEntireTablePageByPage(500, 100));
        }
    }

    @Test
    public void selectEntireTable100B() throws Throwable
    {
        try(TestHelper helper = new TestBuilder(this).numPartitions(1000)
                                                     .numClusterings(100)
                                                     .partitionSize(100)
                                                     .build())
        {
            //warmup
            helper.readEntireTablePageByPage(50, 5000);
            helper.readEntireTableWithBulkPaging(50, 5000);

            logger.info("Total time reading 100B table with bulk paging: {} milliseconds",
                        helper.readEntireTableWithBulkPaging(100, 5000));

            logger.info("Total time reading 100B table page by page: {} milliseconds",
                        helper.readEntireTablePageByPage(100, 5000));
        }
    }

    private static class CustomNettyOptions extends NettyOptions
    {
        private final int numThreads; // zero means use the netty default value

        public CustomNettyOptions(int numThreads)
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

        public TestBuilder(CQLTester tester)
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
                List<CompletableFuture<Void>> results = new ArrayList<>();
                ListenableFuture<ResultSet> resultFuture = session.executeAsync(statement);
                final CheckResultSet checker = new CheckResultSet();
                while (resultFuture != null)
                {
                    final ResultSet resultSet = resultFuture.get();
                    if (!resultSet.isFullyFetched()) // the best we can do here is start fetching before processing
                        resultFuture = resultSet.fetchMoreResults(); // this batch of results
                    else
                        resultFuture = null;

                    maybePauseClient();
                    results.add(CompletableFuture.runAsync(() -> checker.checkPage(resultSet)));
                }

                //Now wait for the page futures to complete
                for (CompletableFuture result : results)
                    result.join();

                checker.checkAll();
            }

            return (System.nanoTime() - start) / (1000000 * numTrials);
        }

        private long readEntireTableWithBulkPaging(int numTrials, int pageSizeRows) throws Throwable
        {
            long start = System.nanoTime();
            for (int i = 0; i < numTrials; i++)
            {
                Session session = tester.sessionNet(protocolVersion);
                Statement statement = new SimpleStatement(tester.formatQuery("SELECT k, c, val1, val2 FROM %s"));

                try(ResultSetIterator it = session.executeAsync(statement, AsyncPagingOptions.create(pageSizeRows)))
                {
                    //Schedule the conversions
                    List<CompletableFuture<?>> results = new ArrayList<>();
                    final CheckResultSet checker = new CheckResultSet();
                    while (it.hasNext())
                    {
                        ResultSet resultSet = it.next();
                        maybePauseClient();
                        results.add(CompletableFuture.runAsync(() -> checker.checkPage(resultSet)));
                    }

                    //Now wait for the page futures to complete
                    for (CompletableFuture result : results)
                        result.join();

                    checker.checkAll();
                }
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
            private final List<Object[]> rowsReceived;
            private int numRowsReceived;

            public CheckResultSet()
            {
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
            }

            private void checkAll()
            {
                assertEquals(rows.length, numRowsReceived);
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
