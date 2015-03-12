import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bryn on 06/03/15.
 */
@AxisRange(min = 0, max = 60)
@BenchmarkMethodChart(filePrefix = "wide-queries")
@RunWith(CassandraRunner.class)
public class WideRowsBenchmark {
    private static Logger log = LoggerFactory.getLogger(WideRowsBenchmark.class);
    private static int maxRecords = 10000000;
    private static int searches = 100000;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private AtomicInteger retrievalCount = new AtomicInteger(0);

    @BeforeClass
    public static void setupData() {

        try {
            QueryProcessor.executeInternal("SELECT * FROM vertexWide WHERE community = 1 AND vertex_id = ?", 0L).iterator().hasNext();

        } catch (InvalidRequestException e) {
            insertVertexData();
        }

    }

    @Before
    public void reset() {
        retrievalCount = new AtomicInteger(0);
    }

    private static void insertVertexData() {
        QueryProcessor.executeInternal("CREATE TABLE IF NOT EXISTS vertexWide (" +
                "community bigint," +
                "vertex_id bigint," +
                "name text," +
                "age int," +
                "PRIMARY KEY (community, vertex_id));");

        log.info("Starting insert vertex data");
        Random random = new Random();
        for (int count = 0; count < maxRecords; count++) {
            if (count % 1000 == 0) {
                log.info("Inserted {}", count);
            }
            QueryProcessor.executeInternal("INSERT INTO vertexWide (community, vertex_id, name, age) VALUES (1, ?, ?, ?);", (long) count, "name" + random.nextLong(), random.nextInt(100));
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testAWarmup() {
        Random random = new Random(0);
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);

            QueryProcessor.executeInternal("SELECT * FROM vertexWide WHERE community = 1 AND vertex_id = ?", (long) random.nextInt(maxRecords)).iterator().forEachRemaining(r -> {
                retrievalCount.incrementAndGet();
            });
        }
        Assert.assertEquals(searches, retrievalCount.get());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRepeatedRows() {
        Random random = new Random(0);
        log.info("Querying repeated");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);
            QueryProcessor.executeInternal("SELECT * FROM vertexWide WHERE community = 1 AND vertex_id = ?", (long) random.nextInt(100)).iterator().forEachRemaining(r -> {
                retrievalCount.incrementAndGet();
            });
        }
        Assert.assertEquals(searches, retrievalCount.get());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testSequentialRows() {
        log.info("Querying sequentially");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);
            QueryProcessor.executeInternal("SELECT * FROM vertexWide WHERE community = 1 AND vertex_id = ?", (long) count).iterator().forEachRemaining(r -> {
                retrievalCount.incrementAndGet();
            });
        }
        Assert.assertEquals(searches, retrievalCount.get());
    }


    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testLimitQuery() {
        log.info("Querying with limit");
        QueryProcessor.executeInternal("SELECT * FROM vertexWide LIMIT ?", searches).iterator().forEachRemaining(r -> {
            retrievalCount.incrementAndGet();
        });
        Assert.assertEquals(searches, retrievalCount.get());
    }


    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithIn10() {
        log.info("Querying with IN");

        queryWithInClause(10, 1);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithIn50() {
        log.info("Querying with IN");

        queryWithInClause(50, 1);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithIn250() {
        log.info("Querying with IN");

        queryWithInClause(250, 1);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithIn1000() {
        log.info("Querying with IN");

        queryWithInClause(1000, 1);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithThreads1() {
        queryWithInClause(1, 1);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithThreads2() {
        queryWithInClause(1, 2);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithThreads4() {
        queryWithInClause(1, 4);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithIn250Threads2() {
        queryWithInClause(250, 2);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testQueryWithIn250Threads4() {
        queryWithInClause(250, 4);
    }

    private void queryWithInClause(int inSize, int threads) {

        CountDownLatch latch = new CountDownLatch(threads);


        for (int thread = 0; thread < threads; thread++) {
            final int threadNumber = thread + 1;
            Runnable r = () -> {
                for (int count = inSize * threadNumber; count < maxRecords && count < searches + 1; count += inSize * threads) {

                    logProgress(count);
                    Object[] ids = new Object[inSize];
                    for (int offset = 0; offset < inSize; offset++) {
                        ids[offset] = new Long(count + offset);
                    }

                    QueryProcessor.executeInternal("SELECT * FROM vertexWide WHERE community = 1 AND vertex_id IN (" + StringUtils.repeat("?, ", inSize - 1) + "?)", ids).iterator().forEachRemaining(row -> {
                        retrievalCount.incrementAndGet();
                    });
                }
                latch.countDown();
            };

            if (threads > 1) {
                new Thread(r).start();
            } else {
                r.run();
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(searches, retrievalCount.get());
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRandomRows() {
        log.info("Querying randomly");
        Random random = new Random(0);
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);

            QueryProcessor.executeInternal("SELECT * FROM vertexWide WHERE community = 1 AND vertex_id = ?", (long) random.nextInt(maxRecords)).iterator().forEachRemaining(r -> {
                retrievalCount.incrementAndGet();
            });
        }
        Assert.assertEquals(searches, retrievalCount.get());
    }


    private void logProgress(int count) {
        if (count % 1000 == 0) {
            log.info("Queried {}", count);
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRandomRowsPreped() {
        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement("SELECT * FROM microbenchmark.vertexWide WHERE community = 1 AND vertex_id = ?", QueryState.forInternalCalls());
        log.info("Querying randomly using prepared statments");
        Random random = new Random(0);
        QueryState queryState = QueryState.forInternalCalls();
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);
            ResultMessage result = prepared.statement.execute(queryState, QueryOptions.create(ConsistencyLevel.ONE, Arrays.asList(LongType.instance.decompose((long) random.nextInt(maxRecords))), true, 1, null, null));
            UntypedResultSet rows = UntypedResultSet.create(((ResultMessage.Rows) result).result);
            rows.forEach(r -> {
                retrievalCount.incrementAndGet();
            });
        }

        Assert.assertEquals(searches, retrievalCount.get());

    }
}
