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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.AbstractVoldemortTest;
import voldemort.store.FailingStore;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.distributed.DistributedStoreFactory;
import voldemort.store.distributed.DistributedStoreTest;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

public class ReadRepairerStoreTest extends AbstractVoldemortTest<String> {

    ExecutorService threadPool;

    public ReadRepairerStoreTest() {
        threadPool = Executors.newFixedThreadPool(5);
    }

    private Map<Integer, Store<String, String>> buildMemoryStores(String name, int count) {
        Map<Integer, Store<String, String>> stores = Maps.newHashMap();
        for(int i = 0; i < count; i++) {
            stores.put(i, new InMemoryStorageEngine<String, String>(name + "_" + i));
        }
        return stores;
    }

    private Map<Integer, Store<String, String>> buildSleepyStores(Map<Integer, Store<String, String>> stores,
                                                                  int count,
                                                                  long delay) {
        Map<Integer, Store<String, String>> sleepies = Maps.newHashMap();
        sleepies.putAll(stores);
        for(int i = 0; i < count; i++) {
            sleepies.put(i, new SleepyStore<String, String>(delay, stores.get(i)));
        }
        return sleepies;
    }

    private Map<Integer, AsynchronousStore<String, String>> toAsyncStores(Map<Integer, Store<String, String>> stores) {
        Map<Integer, AsynchronousStore<String, String>> async = Maps.newHashMap();
        for(Map.Entry<Integer, Store<String, String>> entry: stores.entrySet()) {
            async.put(entry.getKey(),
                      DistributedStoreTest.toThreadedStore(entry.getValue(), threadPool));
        }
        return async;
    }

    private void awaitCompletion(DistributedStore<?, ?, ?> store) {
        try {
            ReadRepairStore<?, ?, ?> readRepair = (ReadRepairStore<?, ?, ?>) store;
            while((readRepair.pendingRepairs.intValue()) > 0) {
                Thread.sleep(100);
            }

        } catch(Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    private void awaitCompletion(DistributedStore<?, ?, ?> store, StoreFuture<?> future) {
        try {
            DistributedFuture<?, ?> distributed = (DistributedFuture<?, ?>) future;
            distributed.complete();
            awaitCompletion(store);
        } catch(Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    private StoreDefinition getStoreDef(String name, int replicas) {
        return getStoreDef(name, replicas, (replicas + 1) / 2, 1);
    }

    private StoreDefinition getStoreDef(String name, int replicas, int preferred, int required) {
        return ServerTestUtils.getStoreDef(name,
                                           replicas,
                                           preferred,
                                           required,
                                           preferred,
                                           required,
                                           RoutingStrategyType.CONSISTENT_STRATEGY);
    }

    private DistributedStore<Integer, String, String> getRepairStore(Map<Integer, Store<String, String>> stores,
                                                                     StoreDefinition storeDef) {
        Map<Integer, AsynchronousStore<String, String>> asyncStores = toAsyncStores(stores);
        return DistributedStoreFactory.create(storeDef, asyncStores, true);

    }

    private void assertReadRepair(Map<String, List<Versioned<String>>> values,
                                  Map<Integer, Store<String, String>> stores) {
        for(String key: values.keySet()) {
            for(Store<String, String> store: stores.values()) {
                try {
                    List<Versioned<String>> expected = values.get(key);
                    List<Versioned<String>> results = store.get(key);
                    assertEquals("Sizes match for store " + store.getName() + " for key " + key,
                                 expected.size(),
                                 results.size());
                    for(Versioned<String> result: results) {
                        this.assertContains(expected, result);
                    }
                } catch(VoldemortException e) {
                    fail("Unexpected exception in store " + store.getName() + " for key " + key
                         + ":" + e.getMessage());
                }
            }
        }
    }

    /**
     * See Issue 150: Missing keys are not added to node when performing
     * read-repair
     */
    @Test
    public void testMissingKeysAreAddedToNodeWhenDoingReadRepair() throws Exception {
        Map<Integer, Store<String, String>> stores = buildMemoryStores("repair", 5);
        StoreDefinition storeDef = getStoreDef("repair",
                                               stores.size(),
                                               stores.size(),
                                               stores.size());
        DistributedStore<Integer, String, String> repair = getRepairStore(stores, storeDef);
        AsynchronousStore<String, String> async = DistributedStoreFactory.asAsync(storeDef, repair);

        Collection<String> keys = Arrays.asList("1", "2");
        Versioned<String> one = new Versioned<String>("one", TestUtils.getClock(1));
        Versioned<String> two = new Versioned<String>("two", TestUtils.getClock(2));
        Versioned<String> value = new Versioned<String>("value", TestUtils.getClock(3, 3));

        stores.get(2).put("key", value);
        stores.get(3).put("key", value);
        stores.get(4).put("key", value);
        stores.get(2).put("1", one);
        stores.get(3).put("1", one);
        stores.get(4).put("1", one);
        stores.get(0).put("2", two);
        stores.get(1).put("2", two);
        stores.get(2).put("2", two);

        Map<String, List<Versioned<String>>> expected = stores.get(2)
                                                              .getAll(Collections.singletonList("key"));

        awaitCompletion(repair, async.submitGet("key"));
        assertReadRepair(expected, stores);

        expected = stores.get(2).getAll(keys);
        awaitCompletion(repair, async.submitGetAll(keys));
        assertReadRepair(expected, stores);
    }

    @Test
    public void testSlowStoreDuringReadRepair() {
        Map<Integer, Store<String, String>> stores = buildMemoryStores("repair", 5);
        Map<Integer, Store<String, String>> sleepies = buildSleepyStores(stores, 2, 100);
        StoreDefinition storeDef = getStoreDef("repair", sleepies.size(), sleepies.size(), 1);
        DistributedStore<Integer, String, String> repair = getRepairStore(sleepies, storeDef);
        AsynchronousStore<String, String> async = DistributedStoreFactory.asAsync(storeDef, repair);
        Collection<String> keys = Arrays.asList("1", "2");
        Versioned<String> one = new Versioned<String>("one", TestUtils.getClock(1));
        Versioned<String> two = new Versioned<String>("two", TestUtils.getClock(2));
        Versioned<String> value = new Versioned<String>("value", TestUtils.getClock(3, 3));

        stores.get(2).put("key", value);
        stores.get(3).put("key", value);
        stores.get(4).put("key", value);
        stores.get(2).put("1", one);
        stores.get(3).put("1", one);
        stores.get(4).put("1", one);
        stores.get(0).put("2", two);
        stores.get(1).put("2", two);
        stores.get(2).put("2", two);

        Map<String, List<Versioned<String>>> expected = stores.get(2)
                                                              .getAll(Collections.singletonList("key"));

        awaitCompletion(repair, async.submitGet("key"));
        assertReadRepair(expected, sleepies);

        expected = stores.get(2).getAll(keys);
        awaitCompletion(repair, async.submitGetAll(keys));
        assertReadRepair(expected, sleepies);

    }

    @Test
    public void testReadRepairWithFailures() {
        Map<Integer, Store<String, String>> stores = buildMemoryStores("repair", 5);

        stores = buildSleepyStores(stores, 2, 100);
        Map<Integer, Store<String, String>> failures = Maps.newHashMap(stores);
        failures.put(5,
                     AsyncUtils.asStore(new FailingStore<String, String>("failure",
                                                                         new VoldemortException("oops"))));
        failures.put(6,
                     AsyncUtils.asStore(new FailingStore<String, String>("failure",
                                                                         new VoldemortException("oops"))));
        StoreDefinition storeDef = getStoreDef("failure", failures.size(), failures.size(), 2);
        DistributedStore<Integer, String, String> repair = getRepairStore(failures, storeDef);
        AsynchronousStore<String, String> async = DistributedStoreFactory.asAsync(storeDef, repair);
        Collection<String> keys = Arrays.asList("1", "2");
        Versioned<String> one = new Versioned<String>("one", TestUtils.getClock(1));
        Versioned<String> two = new Versioned<String>("two", TestUtils.getClock(2));
        Versioned<String> value = new Versioned<String>("value", TestUtils.getClock(3, 3));

        stores.get(2).put("key", value);
        stores.get(3).put("key", value);
        stores.get(4).put("key", value);
        stores.get(2).put("1", one);
        stores.get(3).put("1", one);
        stores.get(4).put("1", one);
        stores.get(0).put("2", two);
        stores.get(1).put("2", two);
        stores.get(2).put("2", two);

        Map<String, List<Versioned<String>>> expected = stores.get(2)
                                                              .getAll(Collections.singletonList("key"));

        awaitCompletion(repair, async.submitGet("key"));
        assertReadRepair(expected, stores);

        expected = stores.get(2).getAll(keys);
        awaitCompletion(repair, async.submitGetAll(keys));
        assertReadRepair(expected, stores);

    }

    @Test
    public void testReadRepairWithOnlyRequiredResponses() throws Exception {
        Map<Integer, Store<String, String>> stores = buildMemoryStores("required", 5); // 3
        // good
        // stores
        stores = buildSleepyStores(stores, 2, 1000); // 2 sleepy ones

        StoreDefinition storeDef = getStoreDef("repair", stores.size(), 5, 1);
        DistributedStore<Integer, String, String> repair = getRepairStore(stores, storeDef);

        Collection<String> keys = Arrays.asList("1", "2");
        Versioned<String> one = new Versioned<String>("one", TestUtils.getClock(1));
        Versioned<String> two = new Versioned<String>("two", TestUtils.getClock(2));
        Versioned<String> value = new Versioned<String>("value", TestUtils.getClock(3, 3));

        stores.get(2).put("key", value);
        stores.get(3).put("key", value);
        stores.get(4).put("key", value);
        stores.get(2).put("1", one);
        stores.get(3).put("1", one);
        stores.get(4).put("1", one);
        stores.get(0).put("2", two);
        stores.get(1).put("2", two);
        stores.get(2).put("2", two);

        DistributedFuture<Integer, List<Versioned<String>>> f = repair.submitGet("key",
                                                                                 stores.keySet(),
                                                                                 5,
                                                                                 3);
        try {
            List<Versioned<String>> expected = f.get(500, TimeUnit.MILLISECONDS);
            Collection<Integer> responders = f.getResults().keySet();
            awaitCompletion(repair);
            for(Integer r: responders) {
                List<Versioned<String>> result = stores.get(r).get("key");
                assertEquals("Same number of results", expected.size(), result.size());
                for(Versioned<String> v: expected) {
                    assertContains(result, v);
                }
            }
        } catch(Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }
}
