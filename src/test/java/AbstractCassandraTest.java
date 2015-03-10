import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.EmbeddedCassandraService;

import java.io.File;

/**
 * Created by bryn on 06/03/15.
 */
public abstract class AbstractCassandraTest {

    private static final CassandraDaemon cassandraDaemon;

    static {

        try {
            File homeDir = new File(System.getProperty("user.dir"));
            File storageDir = new File(homeDir, "benchmark");

            storageDir.mkdirs();
            // start server internals
            System.setProperty("cassandra.config", Benchmark.class.getResource("cassandra.yaml").toString());
            System.setProperty("cassandra.storagedir", storageDir.toString());
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.init(null);
            cassandraDaemon.start();

        }catch(Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static CassandraDaemon getCassandraDaemon() {
        return cassandraDaemon;
    }
}
