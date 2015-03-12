import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.UserType;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;

/**
 * Created by bryn on 06/03/15.
 */
@AxisRange(min = 0, max = 60)
@BenchmarkMethodChart(filePrefix = "collection-queries")
@RunWith(CassandraRunner.class)
@Ignore
public class CollectionsBenchmark {
    private static Logger log = LoggerFactory.getLogger(CollectionsBenchmark.class);
    private static int maxRecords = 10000000;
    private static int largeMaxEdges = 10000;
    private static int smallMaxEdges = 100;
    private static int searches = 100000;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    @BeforeClass
    public static void setupData() {

        if (!QueryProcessor.executeInternal("SELECT * FROM edge WHERE vertex_id = ?", 0L).iterator().hasNext()) {
            insertEdgeData();
        }
    }




    private static void insertEdgeData() {


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
                follows.forEach((k,v)->{});

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
