import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by bryn on 06/03/15.
 */
@AxisRange(min = 0, max = 60)
@BenchmarkMethodChart(filePrefix = "benchmark-queries")
public class Benchmark extends AbstractCassandraTest {
    private static Logger log = LoggerFactory.getLogger(Benchmark.class);
    private static int maxRecords = 10000000;
    private static int largeMaxEdges = 10000;
    private static int smallMaxEdges = 100;
    private static int searches = 100000;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BeforeClass
    public static void setupData() {
        try {
            QueryProcessor.executeInternal("USE microbenchmark;");
        }
        catch(KeyspaceNotDefinedException e) {
            prepareKeyspace();
        }
        if (!QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", 0L).iterator().hasNext()) {
            insertVertexData();
            insertEdgeData();
        }
    }

    @AfterClass
    public static void shutdown() {
        getCassandraDaemon().stop();
    }


    private static void prepareKeyspace() {
        QueryProcessor.executeInternal("CREATE KEYSPACE IF NOT EXISTS microbenchmark WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        QueryProcessor.executeInternal("USE microbenchmark;");

        QueryProcessor.executeInternal("CREATE TABLE IF NOT EXISTS vertex (" +
                "vertex_id bigint," +
                "name text," +
                "age int," +
                "PRIMARY KEY (vertex_id));");

        QueryProcessor.executeInternal("CREATE TYPE IF NOT EXISTS edge_key (" +
                "name text," +
                "edge_id bigint);");

        QueryProcessor.executeInternal("CREATE TYPE IF NOT EXISTS edge_value (" +
                "vertex_id bigint," +
                "occupation text);");

        QueryProcessor.executeInternal("CREATE TABLE IF NOT EXISTS edge (" +
                "vertex_id bigint," +
                "follows map<frozen<edge_key>, frozen<edge_value>>," +
                "PRIMARY KEY (vertex_id));");
    }

    private static void insertVertexData() {
        log.info("Starting insert vertex data");
        Random random = new Random();
        for (int count = 0; count < maxRecords; count++) {
            if (count % 1000 == 0) {
                log.info("Inserted {}", count);
            }
            QueryProcessor.executeInternal("INSERT INTO vertex (vertex_id, name, age) VALUES (?, ?, ?);", (long) count, "name" + random.nextLong(), random.nextInt(100));
        }
    }


    private static void insertEdgeData() {
        log.info("Starting insert edge data");
        Random random = new Random(0);
        for (int count = 0; count < 2000; count++) {
            if (count % 1000 == 0) {
                log.info("Inserted {}", count);
            }
            QueryProcessor.executeInternal("INSERT INTO edge (vertex_id) VALUES (?);", (long) count);

            int maxEdges = count < 1000 ? largeMaxEdges : smallMaxEdges;
            for (int edgeCount = 0; edgeCount < maxEdges; edgeCount++) {
                if (maxEdges > 100 && edgeCount % 1000 == 0) {
                    log.info("Inserted edge map entries {}", edgeCount);
                }
                QueryProcessor.executeInternal("UPDATE edge SET follows[{name:?, edge_id:?}] = {vertex_id:?, occupation:?} WHERE vertex_id = ?;", "edgename" + random.nextLong(), (long) edgeCount, (long) count, "occupaton " + count, (long) count);
            }
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRepeatedRows() {
        Random random = new Random();
        log.info("Querying repeated");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }
            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", (long) random.nextInt(100)).iterator().forEachRemaining(r -> {
            });
        }

    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testSequentialRows() {
        log.info("Querying sequentially");
        for (int count = 0; count < maxRecords && count < searches; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }
            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id = ?", (long) count).iterator().forEachRemaining(r -> {
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
//            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE token(vertex_id) >= token(?) AND token(vertex_id) < token(?)", (long)count, (long)count + 100L).iterator().forEachRemaining(r->{});
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
    public void testMultiQuerySmall() {
        log.info("Querying with IN");
        int inSize = 10;
        for (int count = 0; count < maxRecords && count < searches / inSize; count += inSize) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }

            Object[] ids = new Object[inSize];
            for(int offset = 0; offset < inSize; offset++) {
                ids[offset] = new Long(count + offset);
            }

            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id IN (" + StringUtils.repeat("?, ", inSize - 1) + " ?)", ids).iterator().forEachRemaining(r -> {
            });
        }

    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testMultiQueryLarge() {
        log.info("Querying with IN");
        int inSize = 100;
        for (int count = 0; count < maxRecords && count < searches / inSize; count += inSize) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }

            Object[] ids = new Object[inSize];
            for(int offset = 0; offset < inSize; offset++) {
                ids[offset] = new Long(count + offset);
            }

            QueryProcessor.executeInternal("SELECT * FROM vertex WHERE vertex_id IN (" + StringUtils.repeat("?, ", inSize - 1) + "?)", ids).iterator().forEachRemaining(r -> {
            });
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testRandomRows() {
        log.info("Querying randomly");
        Random random = new Random();
        for (int count = 0; count < maxRecords && count < searches; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }

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


    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testLargeCollections() {
        log.info("Querying large collection");
        UserType keyType = Schema.instance.getKSMetaData("microbenchmark").userTypes.getType(ByteBuffer.wrap(new String("edge_key").getBytes()));
        UserType valueType = Schema.instance.getKSMetaData("microbenchmark").userTypes.getType(ByteBuffer.wrap(new String("edge_value").getBytes()));
        for (int count = 0; count < maxRecords && count < searches / largeMaxEdges; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }

            QueryProcessor.executeInternal("SELECT * FROM edge WHERE vertex_id = ?", (long) (count % 1000)).iterator().forEachRemaining(r -> {
                Map<ByteBuffer, ByteBuffer> follows = r.getMap("follows", keyType, valueType);

            });
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    public void testSmallCollections() {
        log.info("Querying small collection");
        UserType keyType = Schema.instance.getKSMetaData("microbenchmark").userTypes.getType(ByteBuffer.wrap(new String("edge_key").getBytes()));
        UserType valueType = Schema.instance.getKSMetaData("microbenchmark").userTypes.getType(ByteBuffer.wrap(new String("edge_value").getBytes()));

        for (int count = 0; count < maxRecords && count < searches / smallMaxEdges; count++) {
            if (count % 1000 == 0) {
                log.info("Queried {}", count);
            }

            QueryProcessor.executeInternal("SELECT * FROM edge WHERE vertex_id = ?", (long) (count % 1000) + 1000).iterator().forEachRemaining(r -> {
                Map<ByteBuffer, ByteBuffer> follows = r.getMap("follows", keyType, valueType);

            });
        }
    }

}
