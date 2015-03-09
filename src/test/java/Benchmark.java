import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by bryn on 06/03/15.
 */
@AxisRange(min = 0, max = 60)
@BenchmarkMethodChart(filePrefix = "benchmark-queries")
public class Benchmark extends AbstractCassandraTest {
    private static Logger log = LoggerFactory.getLogger(Benchmark.class);
    private static int maxRecords = 10000000;
    private static int searches = 100000;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BeforeClass
    public static void setupData() {
        prepareKeyspace();
        insertData();

    }

    private static void prepareKeyspace() {
        QueryProcessor.executeInternal("CREATE KEYSPACE microbenchmark WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        QueryProcessor.executeInternal("USE microbenchmark;");
        QueryProcessor.executeInternal("CREATE TABLE vertex (" +
                "vertexid bigint," +
                "name text," +
                "age int," +
                " PRIMARY KEY (vertexid));");
    }

    private static void insertData() {
        log.info("Starting Insert data");
        Random random = new Random();
        for (int count = 0; count < maxRecords; count++) {
            if (count % 1000 == 0) {
                log.info("Inserted {}", count);
            }
            QueryProcessor.executeInternal("INSERT INTO vertex (vertexid, name, age) VALUES (?, ?, ?);", (long) count, "name" + random.nextLong(), random.nextInt(100));
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRepeatedQueries() {
        Random random = new Random();
        log.info("Querying repeated");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }
            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertexId = ?", (long) random.nextInt(100)).iterator().forEachRemaining(r -> {
            });
        }

    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testSequentialQueries() {
        log.info("Querying sequentially");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }
            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertexId = ?", (long) count).iterator().forEachRemaining(r -> {
            });
        }

    }

//  The idea here was to query the rows 100 at a time, but it gets stuck on the third query....
//    @Test
//    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
//    public void testTokenSliceQueries() {
//        log.info("Querying token slices");
//        for(int count = 0; count < maxRecords && count < searches / 100; count++) {
//
//            log.info("Queried {}", count);
//
//            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE token(vertexId) >= token(?) AND token(vertexId) < token(?)", (long)count, (long)count + 100L).iterator().forEachRemaining(r->{});
//        }
//
//    }


    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testLimitQuery() {
        log.info("Querying with limit");
        QueryProcessor.executeInternal("SELECT * FROM vertex LIMIT ?", searches).iterator().forEachRemaining(r -> {
        });

    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRandomQueries() {
        log.info("Querying randomly");
        Random random = new Random();
        for (int count = 0; count < maxRecords && count < searches; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }

            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertexId = ?", (long) random.nextInt(maxRecords)).iterator().forEachRemaining(r -> {
            });
        }

    }


    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testStorageProxyQuery() {
        log.info("Querying via storage proxy");
        CFMetaData metadata = Schema.instance.getCFMetaData("microbenchmark", "vertex");

        for (int count = 0; count < maxRecords && count < searches; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }

            List<ReadCommand> commands = new ArrayList<ReadCommand>();


            IDiskAtomFilter filter = new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 1);
            ReadCommand readCommand = ReadCommand.create("microbenchmark", ByteBufferUtil.bytes((long) count), "vertex", System.currentTimeMillis(), filter);

            commands.add(readCommand);
            List<Row> rows = StorageProxy.read(commands, ConsistencyLevel.ONE);

        }


    }


}
