import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.service.CassandraDaemon;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;

/**
 * Created by bryn on 11/03/15.
 */
public class CassandraRunner extends BlockJUnit4ClassRunner {


    private static CassandraDaemon cassandraDaemon;

    public CassandraRunner(Class testClass) throws InitializationError {
        super(testClass);

    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cassandraDaemon.stop();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        setupCassandra();


    }



    private static void setupCassandra() {
        try {
            File homeDir = new File(System.getProperty("user.dir"));
            File storageDir = new File(homeDir, "benchmark");

            storageDir.mkdirs();
            // start server internals
            System.setProperty("cassandra.config", SkinnyRowsBenchmark.class.getResource("cassandra.yaml").toString());
            System.setProperty("cassandra.storagedir", storageDir.toString());
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.init(null);
            cassandraDaemon.start();
            prepareKeyspace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void prepareKeyspace() {
        try {
            QueryProcessor.executeInternal("CREATE KEYSPACE IF NOT EXISTS microbenchmark WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
            QueryProcessor.executeInternal("USE microbenchmark;");
        }
        catch(AssertionError e){
            QueryProcessor.executeInternal("USE microbenchmark;");
        }

    }

}
