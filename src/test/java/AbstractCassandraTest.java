import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;

import java.io.File;

/**
 * Created by bryn on 06/03/15.
 */
public abstract class AbstractCassandraTest {

    static {

        try {
            File homeDir = new File(System.getProperty("user.dir"));
            File storageDir = new File(homeDir, "benchmark");
            if(storageDir.exists()) {
                FileUtils.deleteRecursive(storageDir);
            }
            storageDir.mkdirs();
            // start server internals
            System.setProperty("cassandra.config", Benchmark.class.getResource("cassandra.yaml").toString());
            System.setProperty("cassandra.storagedir", storageDir.toString());
            System.setProperty("cassandra-foreground", "yes");
            EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
            cassandra.start();
        }catch(Exception e) {
            throw new RuntimeException(e);
        }

    }


}
