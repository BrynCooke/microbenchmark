import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by bryn on 06/03/15.
 */
@AxisRange(min = 0, max = 60)
@BenchmarkMethodChart(filePrefix = "benchmark-queries")
@RunWith(CassandraRunner.class)
public class SkinnyRowsBenchmark {
    private static Logger log = LoggerFactory.getLogger(SkinnyRowsBenchmark.class);
    private static int maxRecords = 10000000;
    private static int searches = 100000;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BeforeClass
    public static void setupData() {


        if (!QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", 0L).iterator().hasNext()) {
            insertVertexData();
        }
    }


    private static void insertVertexData() {
        QueryProcessor.executeInternal("CREATE TABLE IF NOT EXISTS vertex (" +
                "vertex_id bigint," +
                "name text," +
                "age int," +
                "PRIMARY KEY (vertex_id));");

        log.info("Starting insert vertex data");
        Random random = new Random();
        for (int count = 0; count < maxRecords; count++) {
            if (count % 1000 == 0) {
                log.info("Inserted {}", count);
            }
            QueryProcessor.executeInternal("INSERT INTO vertex (vertex_id, name, age) VALUES (?, ?, ?);", (long) count, "name" + random.nextLong(), random.nextInt(100));
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testAWarmup() {
        Random random = new Random();
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);

            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", (long) random.nextInt(maxRecords)).iterator().forEachRemaining(r -> {
            });
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRepeatedRows() {
        Random random = new Random();
        log.info("Querying repeated");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);
            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", (long) random.nextInt(100)).iterator().forEachRemaining(r -> {
            });
        }

    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testSequentialRows() {
        log.info("Querying sequentially");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);
            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", (long) count).iterator().forEachRemaining(r -> {
            });
        }

    }


    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testLimitQuery() {
        log.info("Querying with limit");
        QueryProcessor.executeInternal("SELECT * FROM vertex LIMIT ?", searches).iterator().forEachRemaining(r -> {
        });

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
        Runnable r = () -> {
            for (int count = 0; count < maxRecords && count < searches / inSize / threads; count += inSize) {
                logProgress(count);

                Object[] ids = new Object[inSize];
                for (int offset = 0; offset < inSize; offset++) {
                    ids[offset] = new Long(count + offset);
                }

                QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id IN (" + StringUtils.repeat("?, ", inSize - 1) + "?)", ids).iterator().forEachRemaining(row -> {
                });
            }
            latch.countDown();
        };

        for( int thread = 0; thread < threads; thread++) {
            if(threads > 1) {
                new Thread(r).start();
            }
            else {
                r.run();
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRandomRows() {
        log.info("Querying randomly");
        Random random = new Random();
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);

            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", (long) random.nextInt(maxRecords)).iterator().forEachRemaining(r -> {
            });
        }

    }


    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testStorageProxy() {
        log.info("Querying via storage proxy");
        CFMetaData metadata = Schema.instance.getCFMetaData("microbenchmark", "vertex");

        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);

            List<ReadCommand> commands = new ArrayList<ReadCommand>();


            IDiskAtomFilter filter = new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 1);
            ReadCommand readCommand = ReadCommand.create("microbenchmark", LongType.instance.decompose((long) count), "vertex", System.currentTimeMillis(), filter);

            commands.add(readCommand);
            List<Row> rows = StorageProxy.read(commands, ConsistencyLevel.ONE, ClientState.forInternalCalls());

        }


    }

    private void logProgress(int count) {
        if (count % 1000 == 0) {
            log.info("Queried {}", count);
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRandomRowsPreped() {
        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement("SELECT * FROM microbenchmark.vertex WHERE vertex_id = ?", QueryState.forInternalCalls());
        log.info("Querying randomly using prepared statments");
        Random random = new Random();
        for (int count = 0; count < maxRecords && count < searches; count++) {
            logProgress(count);
            ResultMessage result = prepared.statement.execute(QueryState.forInternalCalls(), QueryOptions.create(ConsistencyLevel.ONE, Arrays.asList(LongType.instance.decompose((long) random.nextInt(maxRecords))), true, 1, null, null));
            UntypedResultSet rows = UntypedResultSet.create(((ResultMessage.Rows) result).result);
            rows.forEach(r->{});
        }


    }
}
