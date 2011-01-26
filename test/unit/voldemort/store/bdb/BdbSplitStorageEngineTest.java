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

package voldemort.store.bdb;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;

/**
 * checks that
 * 
 * 
 */
public class BdbSplitStorageEngineTest extends TestCase {

    private File bdbMasterDir;
    private BdbStorageConfiguration bdbStorage;

    private static long CACHE_SIZE = (long) Math.min(Runtime.getRuntime().maxMemory() * 0.30,
                                                     32 * 1000 * 1000);

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        bdbMasterDir = TestUtils.createTempDir();
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            if(bdbStorage != null)
                bdbStorage.close();
        } finally {
            try {
                FileDeleteStrategy.FORCE.delete(bdbMasterDir);
            } catch(Exception e) {
                System.out.println("Failed to delete " + bdbMasterDir + ": " + e.getMessage());
            }
        }
    }

    protected VoldemortConfig getVoldemortConfig(boolean oneEnvPerStore) {
        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", this.bdbMasterDir.toURI().getPath());
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(1 * 1024 * 1024);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
        voldemortConfig.setBdbOneEnvPerStore(oneEnvPerStore);
        return voldemortConfig;
    }

    public void testNoMultipleEnvironment() {
        VoldemortConfig voldemortConfig = getVoldemortConfig(false);
        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        BdbStorageEngine storeA = (BdbStorageEngine) bdbStorage.getStore(TestUtils.getStoreDef("storeA",
                                                                                               BdbStorageConfiguration.TYPE_NAME));
        BdbStorageEngine storeB = (BdbStorageEngine) bdbStorage.getStore(TestUtils.getStoreDef("storeB",
                                                                                               BdbStorageConfiguration.TYPE_NAME));

        storeA.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeB.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeA.close();
        storeB.close();

        assertEquals("common BDB file should exists.", true, (bdbMasterDir.exists()));

        assertNotSame("StoreA BDB file should not exists.", true, (new File(bdbMasterDir + "/"
                                                                            + "storeA").exists()));
        assertNotSame("StoreB BDB file should not exists.", true, (new File(bdbMasterDir + "/"
                                                                            + "storeB").exists()));
    }

    public void testMultipleEnvironment() {
        VoldemortConfig voldemortConfig = this.getVoldemortConfig(true);

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        BdbStorageEngine storeA = (BdbStorageEngine) bdbStorage.getStore(TestUtils.getStoreDef("storeA",
                                                                                               BdbStorageConfiguration.TYPE_NAME));
        BdbStorageEngine storeB = (BdbStorageEngine) bdbStorage.getStore(TestUtils.getStoreDef("storeB",
                                                                                               BdbStorageConfiguration.TYPE_NAME));

        storeA.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeB.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeA.close();
        storeB.close();

        assertEquals("StoreA BDB file should exists.", true, (new File(bdbMasterDir + "/"
                                                                       + "storeA").exists()));
        assertEquals("StoreB BDB file should  exists.", true, (new File(bdbMasterDir + "/"
                                                                        + "storeB").exists()));
    }

    public void testUnsharedCache() throws DatabaseException {
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTxnNoSync(true);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        environmentConfig.setSharedCache(false);
        environmentConfig.setCacheSize(CACHE_SIZE);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(true);

        long maxCacheSize = getMaxCacheUsage(environmentConfig, databaseConfig, true);

        assertEquals("" + maxCacheSize + ">" + CACHE_SIZE, true, maxCacheSize > CACHE_SIZE);
        assertEquals("MaxCacheSize < 2 * CACHE_SIZE", true, maxCacheSize < 2 * CACHE_SIZE);
    }

    public void testSharedCache() throws DatabaseException {
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setTxnNoSync(true);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        environmentConfig.setCacheSize(CACHE_SIZE);
        environmentConfig.setSharedCache(true);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(true);

        long maxCacheSize = getMaxCacheUsage(environmentConfig, databaseConfig, true);
        assertEquals("MaxCacheSize <= CACHE_SIZE", true, maxCacheSize <= CACHE_SIZE);
    }

    private long getMaxCacheUsage(EnvironmentConfig environmentConfig,
                                  DatabaseConfig databaseConfig,
                                  boolean onePerStore) throws DatabaseException {
        VoldemortConfig voldemortConfig = this.getVoldemortConfig(onePerStore);

        bdbStorage = new BdbStorageConfiguration(voldemortConfig, environmentConfig, databaseConfig);
        BdbStorageEngine storeA = (BdbStorageEngine) bdbStorage.getStore(TestUtils.getStoreDef("storeA",
                                                                                               BdbStorageConfiguration.TYPE_NAME));

        BdbStorageEngine storeB = (BdbStorageEngine) bdbStorage.getStore(TestUtils.getStoreDef("storeB",
                                                                                               BdbStorageConfiguration.TYPE_NAME));

        long maxCacheUsage = 0;
        for(int i = 0; i <= 4; i++) {

            byte[] value = new byte[(int) (CACHE_SIZE / 4)];
            // try to push values in cache
            storeA.put(TestUtils.toByteArray(i + "A"), new Versioned<byte[]>(value), null);
            storeA.get(TestUtils.toByteArray(i + "A"), null);

            storeB.put(TestUtils.toByteArray(i + "B"), new Versioned<byte[]>(value), null);
            storeB.get(TestUtils.toByteArray(i + "B"), null);

            EnvironmentStats statsA = bdbStorage.getStats("storeA");
            EnvironmentStats statsB = bdbStorage.getStats("storeB");

            long totalCacheSize = statsA.getCacheTotalBytes() + statsB.getCacheTotalBytes();
            System.out.println("A.size:" + statsA.getCacheTotalBytes() + " B.size:"
                               + statsB.getCacheTotalBytes() + " total:" + totalCacheSize + " max:"
                               + maxCacheUsage + " cacheMax:"
                               + storeA.environment.getConfig().getCacheSize());
            System.out.println("Shared.A:" + statsA.getSharedCacheTotalBytes() + " nSharedEnv:"
                               + statsA.getNSharedCacheEnvironments());
            maxCacheUsage = Math.max(maxCacheUsage, totalCacheSize);
        }
        storeA.close();
        storeB.close();
        return maxCacheUsage;
    }
}
