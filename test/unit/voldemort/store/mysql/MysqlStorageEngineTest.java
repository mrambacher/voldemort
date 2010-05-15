/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class MysqlStorageEngineTest extends AbstractStorageEngineTest {

    protected String databaseJDBCHost = null;
    protected String databaseJDBCUser = null;
    protected String databaseJDBCPswd = null;

    protected final static String DBCONN_PROPS = "configs/DBConnection.properties";
    protected final static String DBCONN_HOST_ENV_NAME = "VOLD_JDBC_HOST";
    protected final static String DBCONN_USER_ENV_NAME = "VOLD_JDBC_USER";
    protected final static String DBCONN_PSWD_ENV_NAME = "VOLD_JDBC_PSWD";
    protected final static String DEFAULT_JDBC_HOST = "localhost:3306/test";
    protected final static String DEFAULT_JDBC_USER = "root";
    protected final static String DEFAULT_JDBC_PSWD = "";

    protected MysqlStorageEngineTest(String storeName) {
        super(storeName);
        configureDatabaseProperties();
    }

    public MysqlStorageEngineTest() {
        super("test_store_" + System.getProperty("user.name", "mysql"));
        configureDatabaseProperties();
    }

    @Override
    public void setUp() throws Exception {
        StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
        destroyEngine(engine);
        createEngine(engine);
        super.setUp();
    }

    protected void destroyEngine(StorageEngine<ByteArray, byte[]> engine) {
        MysqlStorageEngine mysql = (MysqlStorageEngine) engine;
        mysql.destroy();
    }

    protected void createEngine(StorageEngine<ByteArray, byte[]> engine) {
        MysqlStorageEngine mysql = (MysqlStorageEngine) engine;
        mysql.create();
    }

    /**
     * If VOLD_JDBC_HOST is set, use it as the <host:port/database> postfix in
     * JDBC connection URL.
     * 
     * Otherwise, use the default from the properties bundle
     * 
     * Finally, use the static constants from this class as the ultimate
     * fallback option
     */
    protected void configureDatabaseProperties() {

        String defaultJDBCHost = null;
        String defaultJDBCUser = null;
        String defaultJDBCPswd = null;

        // Now get all the defaults.
        Properties props = new Properties();
        ClassLoader classLoader = AbstractStorageEngineTest.class.getClassLoader();
        InputStream inStream = null;

        try {
            inStream = classLoader.getResourceAsStream(DBCONN_PROPS);
            if(inStream != null) {
                props.load(inStream);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(null != inStream) {
                try {
                    inStream.close();
                } catch(IOException e1) {
                    e1.printStackTrace();
                }
                inStream = null;
            }
        }
        defaultJDBCHost = props.getProperty("database.jdbc.host", DEFAULT_JDBC_HOST);
        defaultJDBCUser = props.getProperty("database.jdbc.user", DEFAULT_JDBC_USER);
        defaultJDBCPswd = props.getProperty("database.jdbc.password", DEFAULT_JDBC_PSWD);

        String host = System.getenv(DBCONN_HOST_ENV_NAME);
        if(null == host || host.isEmpty()) {
            host = defaultJDBCHost;
        }
        String user = System.getenv(DBCONN_USER_ENV_NAME);
        if(null == user || user.isEmpty()) {
            user = defaultJDBCUser;
        }
        String pswd = System.getenv(DBCONN_PSWD_ENV_NAME);
        if(null == pswd || pswd.isEmpty()) {
            pswd = defaultJDBCPswd;
        }

        databaseJDBCHost = host;
        databaseJDBCUser = user;
        databaseJDBCPswd = pswd;
    }

    @Override
    public StorageEngine<ByteArray, byte[]> createStorageEngine(String name) {
        MysqlStorageEngine engine = new MysqlStorageEngine(name, getDataSource());
        engine.destroy();
        engine.create();
        return engine;
    }

    @Override
    public void tearDown() throws Exception {
        for(String engine: this.engines.keySet()) {
            StorageEngine<ByteArray, byte[]> storageEngine = engines.get(engine);
            destroyEngine(storageEngine);
        }
        super.tearDown();
    }

    @Override
    protected boolean supportsSizes(int keySize, int valueSize) {
        return valueSize < (64 * 1024) && keySize < 200;
    }

    protected DataSource getDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:mysql://" + databaseJDBCHost);
        ds.setUsername(databaseJDBCUser);
        ds.setPassword(databaseJDBCPswd);
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        return ds;
    }

    public boolean executeQuery(String query) throws SQLException {
        DataSource ds = getDataSource();
        Connection c = ds.getConnection();
        PreparedStatement s = c.prepareStatement(query);
        return s.execute();
    }

    @Test
    public void testOpenNonExistantStoreCreatesTable() throws SQLException {
        String newStore = TestUtils.randomLetters(15);
        /* Create the engine for side-effect */
        this.createStorageEngine(newStore);
        executeQuery("select 1 from " + newStore + " limit 1");
        executeQuery("drop table " + newStore);
    }

    @Override
    @Test
    public void testFiveHundredKilobyteSizes() {}
}
