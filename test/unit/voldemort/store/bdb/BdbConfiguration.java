package voldemort.store.bdb;

import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BdbConfiguration {

    private Environment environment;
    private EnvironmentConfig envConfig;
    private DatabaseConfig databaseConfig;

    public BdbConfiguration(File directory, String dbName) throws DatabaseException {
        this.envConfig = new EnvironmentConfig();
        this.envConfig.setTxnNoSync(true);
        this.envConfig.setAllowCreate(true);
        this.envConfig.setTransactional(true);
        this.environment = new Environment(directory, envConfig);
        this.databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(true);
    }

    public BdbStorageEngine createStorageEngine(String storeName) throws DatabaseException {
        if(storeName == null)
            throw new IllegalArgumentException("Store must not be null");
        Database database = environment.openDatabase(null, storeName, databaseConfig);
        return new BdbStorageEngine(storeName, this.environment, database);
    }

    public void close() throws DatabaseException {
        environment.close();
    }
}
