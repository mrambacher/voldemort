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

package voldemort.store.routed;

import static voldemort.FailureDetectorTestUtils.recordException;
import static voldemort.FailureDetectorTestUtils.recordSuccess;
import static voldemort.MutableStoreVerifier.create;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class ReadRepairerStoreTest extends TestCase {

    private final Class<FailureDetector> failureDetectorClass;
    private FailureDetector failureDetector;
    private Time time = new MockTime();

    public ReadRepairerStoreTest(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class } });
    }

    /**
     * See Issue 150: Missing keys are not added to node when performing
     * read-repair
     */

    @Test
    public void testMissingKeysAreAddedToNodeWhenDoingReadRepair() throws Exception {
        ByteArray key = TestUtils.toByteArray("key");
        byte[] value = "foo".getBytes();

        Cluster cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               3,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setBannagePeriod(1000)
                                                                                 .setNodes(cluster.getNodes())
                                                                                 .setTime(time);

        failureDetector = create(failureDetectorConfig, false);

        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        for(int a = 0; a < 3; a++) {
            Node node = Iterables.get(cluster.getNodes(), a);
            subStores.put(node.getId(),
                          new NodeStore<ByteArray, byte[]>(node,
                                                           failureDetector,
                                                           new InMemoryStorageEngine<ByteArray, byte[]>("test")));
        }
        failureDetector.getConfig().setStoreVerifier(create(subStores));

        ReadRepairStore<Integer> store = new ReadRepairStore<Integer>("test",
                                                                      subStores,
                                                                      storeDef,
                                                                      1,
                                                                      1000L,
                                                                      TimeUnit.MILLISECONDS,
                                                                      true);

        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        Versioned<byte[]> versioned = new Versioned<byte[]>(value);
        versioned.getVersion().incrementClock(1);
        store.put(key, versioned);
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        time.sleep(2000);

        assertEquals(2, store.get(key).size());
        // Last get should have repaired the missing key from node 0 so all
        // stores should now return a value
        assertEquals(3, store.get(key).size());

        ByteArray anotherKey = TestUtils.toByteArray("anotherKey");
        // Try again, now use getAll to read repair
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        store.put(anotherKey, versioned);
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        assertEquals(2, store.getAll(Arrays.asList(anotherKey)).get(anotherKey).size());
        assertEquals(3, store.get(anotherKey).size());
    }

    @Test
    public void testSlowStoreDuringReadRepair() {
        ByteArray key = TestUtils.toByteArray("key");
        byte[] value = "foo".getBytes();

        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               3,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        Store<ByteArray, byte[]> sleepy = new InMemoryStorageEngine<ByteArray, byte[]>("sleepy");
        subStores.put(0, new InMemoryStorageEngine<ByteArray, byte[]>("test-0"));
        subStores.put(1, new InMemoryStorageEngine<ByteArray, byte[]>("test-1"));
        subStores.put(2, new SleepyStore<ByteArray, byte[]>(1000L, sleepy));

        subStores.get(0).put(key, new Versioned<byte[]>(value, TestUtils.getClock(3, 3)));

        ReadRepairStore<Integer> store = new ReadRepairStore<Integer>("test",
                                                                      subStores,
                                                                      storeDef,
                                                                      1,
                                                                      1200L,
                                                                      TimeUnit.MILLISECONDS,
                                                                      true);
        List<Versioned<byte[]>> results = store.get(key);
        results.get(0).setObject("bar".getBytes());
        try {
            Thread.sleep(2000);
        } catch(Exception e) {}
        List<Versioned<byte[]>> sleepyResults = sleepy.get(key);
        assertEquals("Sleepy store has all results", results.size(), sleepyResults.size());
        assertEquals("Sleepy results unchanged",
                     new String(value),
                     new String(sleepyResults.get(0).getValue()));
    }
}
