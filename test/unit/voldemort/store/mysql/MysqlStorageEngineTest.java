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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class MysqlStorageEngineTest extends AbstractStorageEngineTest {

    public MysqlStorageEngineTest() {
        super("test_store");
    }

    @Override
    public void setUp() throws Exception {
        // this.engine = (MysqlStorageEngine) createStorageEngine("test_store");
        super.setUp();
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
            MysqlStorageEngine mysql = (MysqlStorageEngine) engines.get(engine);
            mysql.destroy();
        }
        super.tearDown();
    }

    private DataSource getDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl("jdbc:mysql://localhost:3306/test");
        ds.setUsername("root");
        ds.setPassword("");
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        return ds;
    }

    public void executeQuery(DataSource datasource, String query) throws SQLException {
        Connection c = datasource.getConnection();
        PreparedStatement s = c.prepareStatement(query);
        s.execute();
    }

    @Test
    public void testOpenNonExistantStoreCreatesTable() throws SQLException {
        String newStore = TestUtils.randomLetters(15);
        /* Create the engine for side-effect */
        new MysqlStorageEngine(newStore, getDataSource());
        DataSource ds = getDataSource();
        executeQuery(ds, "select 1 from " + newStore + " limit 1");
        executeQuery(ds, "drop table " + newStore);
    }

    @Override
    @Test
    public void testFiveHundredKilobyteSizes() {}
}
