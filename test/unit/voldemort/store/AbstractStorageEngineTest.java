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

package voldemort.store;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.StringSerializer;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public abstract class AbstractStorageEngineTest extends AbstractByteArrayStoreTest {

    protected Map<String, StorageEngine<ByteArray, byte[], byte[]>> engines;
    private final String storeType;

    public AbstractStorageEngineTest(String name, String type) {
        super(name);
        this.storeType = type;
        engines = new HashMap<String, StorageEngine<ByteArray, byte[], byte[]>>();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        for(String name: engines.keySet()) {
            closeStorageEngine(engines.get(name));
        }
    }

    protected void closeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        try {
            engine.close();
        } catch(Exception e) {

        }
    }

    @Override
    public Store<ByteArray, byte[], byte[]> getStore(String name) {
        return getStorageEngine(name);
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return getStorageEngine(storeName);
    }

    protected StoreDefinition getStoreDef(String name) {
        return TestUtils.getStoreDef(name, this.storeType);
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine(String name) {
        StorageEngine<ByteArray, byte[], byte[]> engine = engines.get(name);
        if(engine == null) {
            engine = createStorageEngine(getStoreDef(name));
            engines.put(name, engine);
        }
        return engine;
    }

    @Override
    protected void closeStore(String name) {
        StorageEngine<ByteArray, byte[], byte[]> engine = engines.get(name);
        if(engine != null) {
            engines.remove(name);
            engine.close();
        }
    }

    @Override
    public Store<ByteArray, byte[], byte[]> createStore(String name) {
        return getStorageEngine(name);
    }

    public abstract StorageEngine<ByteArray, byte[], byte[]> createStorageEngine(StoreDefinition storeDef);

    protected <K, V, T> Multimap<K, Versioned<V>> testGetEntries(StorageEngine<K, V, T> engine,
                                                                 List<Integer> partitions,
                                                                 VoldemortFilter filter) {
        ClosableIterator<Pair<K, Versioned<V>>> it = null;
        Multimap<K, Versioned<V>> result = LinkedHashMultimap.create();
        try {
            it = engine.entries(partitions, filter, null);
            while(it.hasNext()) {
                Pair<K, Versioned<V>> entry = it.next();
                result.put(entry.getFirst(), entry.getSecond());
            }
        } finally {
            it.close();
        }
        return result;
    }

    protected <K, V, T> Collection<K> testGetKeys(StorageEngine<K, V, T> engine,
                                                  List<Integer> partitions,
                                                  VoldemortFilter filter) {
        Set<K> keys = new HashSet<K>();
        ClosableIterator<K> it = null;
        try {
            it = engine.keys(partitions, filter);
            while(it.hasNext()) {
                K key = it.next();
                if(keys.contains(key)) {

                } else {
                    keys.add(key);
                }
            }
        } finally {
            it.close();
        }
        return keys;
    }

    @Test
    public void testGetNoEntries() {
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        assertEquals("There shouldn't be any entries in this store.",
                     0,
                     testGetEntries(engine, null, new DefaultVoldemortFilter()).size());
    }

    @Test
    public void testGetNoKeys() {
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        assertEquals("There shouldn't be any keys in this store.",
                     0,
                     testGetKeys(engine, null, new DefaultVoldemortFilter()).size());
    }

    @Test
    public void testKeyIterationWithSerialization() {
        StorageEngine<ByteArray, byte[], byte[]> store = getStorageEngine();
        StorageEngine<String, String, String> stringStore = new SerializingStorageEngine<String, String, String>(store,
                                                                                                                 new StringSerializer(),
                                                                                                                 new StringSerializer(),
                                                                                                                 new StringSerializer());
        Set<String> keys = ImmutableSet.of("a", "b", "c", "d", "e");
        for(String key: keys)
            stringStore.put(key, new Versioned<String>(key), null);
        Collection<String> results = testGetKeys(stringStore, null, new DefaultVoldemortFilter());
        assertEquals(keys.size(), results.size());
        assertTrue(keys.containsAll(results));
    }

    @Test
    public void testKeyIterationWithMultipleVersions() {
        final List<ByteArray> keys = this.getKeys(5);
        List<byte[]> values = this.getValues(10);
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        for(int i = 0; i < values.size(); i++) {
            ByteArray key = keys.get(i % 5);
            Version version = TestUtils.getClock(i);
            engine.put(key, new Versioned<byte[]>(values.get(i), version), null);
        }

        Collection<ByteArray> results = testGetKeys(engine, null, new DefaultVoldemortFilter());
        assertEquals(keys.size(), results.size());
        assertTrue(results.containsAll(keys));
    }

    @Test
    public void testEntriesIterationWithMultipleVersions() {
        final List<ByteArray> keys = this.getKeys(5);
        List<byte[]> values = this.getValues(10);
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        for(int i = 0; i < values.size(); i++) {
            ByteArray key = keys.get(i % 5);
            Version version = TestUtils.getClock(i);
            engine.put(key, new Versioned<byte[]>(values.get(i), version), null);
        }

        Multimap<ByteArray, Versioned<byte[]>> results = testGetEntries(engine,
                                                                        null,
                                                                        new DefaultVoldemortFilter());
        assertEquals(values.size(), results.size());
    }

    private static int ITERATION_KEY_COUNT = 2000;

    @Test
    public void testKeyIterationWithFilter() {
        final int SKIP = 2;
        final List<ByteArray> keys = this.getKeys(ITERATION_KEY_COUNT);
        List<byte[]> values = this.getValues(ITERATION_KEY_COUNT);
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        for(int i = 0; i < keys.size(); i++) {
            ByteArray key = keys.get(i);
            engine.put(key, new Versioned<byte[]>(values.get(i)), null);
        }

        VoldemortFilter keyFilter = new VoldemortFilter() {

            public boolean accept(Object obj, Versioned<?> value) {
                int index = keys.indexOf(obj);
                return (index % SKIP) != 0;
            }
        };
        Collection<ByteArray> results = testGetKeys(engine, null, keyFilter);
        assertEquals(ITERATION_KEY_COUNT / SKIP, results.size());
        for(int i = 1; i < keys.size(); i = i + SKIP) {
            assertTrue(results.contains(keys.get(i)));
        }
    }

    protected Cluster getCluster() {
        Node node = new Node(0, "localhost", 6666, 6667, 6668, Lists.newArrayList(0, 1, 2, 3, 4, 5));
        Cluster cluster = new Cluster("test", Collections.singletonList(node));
        return cluster;
    }

    @Test
    public void testKeyIterationWithPartitions() {
        final List<ByteArray> keys = this.getKeys(ITERATION_KEY_COUNT);
        List<byte[]> values = this.getValues(ITERATION_KEY_COUNT);
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        Cluster cluster = getCluster();
        RoutingStrategy strategy = engine.getStoreDefinition().updateRoutingStrategy(cluster);
        for(int i = 0; i < keys.size(); i++) {
            ByteArray key = keys.get(i);
            engine.put(key, new Versioned<byte[]>(values.get(i)), null);
        }

        for(int p = 0; p < cluster.getNumberOfPartitions(); p++) {
            long started = System.currentTimeMillis();
            Collection<ByteArray> results = testGetKeys(engine,
                                                        Collections.singletonList(p),
                                                        new DefaultVoldemortFilter());
            int count = 0;
            for(ByteArray key: keys) {
                int partition = strategy.getPrimaryPartition(key.get());
                if(partition == p) {
                    count++;
                    assertTrue(results.contains(key));
                }
            }
            System.out.println("Partition " + p + " contains " + count + " keys in "
                               + (System.currentTimeMillis() - started) + " ms");
            assertEquals(count, results.size());
        }
    }

    public void testEntriesIterationWithFilter() {
        final List<ByteArray> keys = this.getKeys(ITERATION_KEY_COUNT);
        final List<byte[]> values = this.getValues(ITERATION_KEY_COUNT);
        final int SKIP = 2;
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        Map<ByteArray, Versioned<byte[]>> versioneds = Maps.newHashMap();
        for(int i = 0; i < keys.size(); i++) {
            ByteArray key = keys.get(i);
            Version version = engine.put(key, new Versioned<byte[]>(values.get(i)), null);
            versioneds.put(key, new Versioned<byte[]>(values.get(i), version));
        }

        VoldemortFilter entryFilter = new VoldemortFilter() {

            public boolean accept(Object obj, Versioned<?> value) {
                for(int index = 0; index < values.size(); index++) {
                    if(valuesEqual(values.get(index), (byte[]) value.getValue())) {
                        return (index % SKIP) != 0;
                    }
                }
                return false;
            }
        };
        Multimap<ByteArray, Versioned<byte[]>> results = testGetEntries(engine, null, entryFilter);
        assertEquals(ITERATION_KEY_COUNT / SKIP, results.size());
        for(int i = 1; i < keys.size(); i = i + SKIP) {
            ByteArray key = keys.get(i);
            Collection<Versioned<byte[]>> versions = results.get(key);
            assertContains(versions, versioneds.get(key));
        }
    }

    @Test
    public void testIterationWithSerialization() {
        StorageEngine<ByteArray, byte[], byte[]> store = getStorageEngine();
        StorageEngine<String, String, String> stringStore = SerializingStorageEngine.wrap(store,
                                                                                          new StringSerializer(),
                                                                                          new StringSerializer(),
                                                                                          new StringSerializer());
        Set<String> keys = ImmutableSet.of("a", "b", "c", "d", "e");
        Multimap<String, Versioned<String>> values = LinkedHashMultimap.create();
        for(String key: keys) {
            Versioned<String> value = new Versioned<String>(key);
            Version version = stringStore.put(key, value, null);
            values.put(key, new Versioned<String>(value.getValue(), version, value.getMetadata()));
        }
        int count = this.testGetEntries(stringStore, null, new DefaultVoldemortFilter()).size();
        assertEquals(count, values.size());
    }

    @Test
    public void testEntriesIterationWithPartitions() {
        final List<ByteArray> keys = this.getKeys(ITERATION_KEY_COUNT);
        List<byte[]> values = this.getValues(ITERATION_KEY_COUNT);
        Cluster cluster = getCluster();
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        RoutingStrategy strategy = engine.getStoreDefinition().updateRoutingStrategy(cluster);
        Map<ByteArray, Versioned<byte[]>> versioneds = Maps.newHashMap();
        for(int i = 0; i < keys.size(); i++) {
            ByteArray key = keys.get(i);
            Version version = engine.put(key, new Versioned<byte[]>(values.get(i)), null);
            versioneds.put(key, new Versioned<byte[]>(values.get(i), version));
        }

        System.out.println("Starting fetch");
        for(int p = 0; p < cluster.getNumberOfPartitions(); p++) {
            long started = System.currentTimeMillis();
            Multimap<ByteArray, Versioned<byte[]>> results = testGetEntries(engine,
                                                                            Collections.singletonList(p),
                                                                            new DefaultVoldemortFilter());
            int count = 0;
            for(ByteArray key: keys) {
                if(strategy.getPrimaryPartition(key.get()) == p) {
                    count++;
                    Collection<Versioned<byte[]>> versions = results.get(key);
                    assertContains(versions, versioneds.get(key));
                }
            }
            System.out.println("Partition " + p + " contains " + count + " keys in "
                               + (System.currentTimeMillis() - started) + " ms");
            assertEquals(count, results.size());
        }
    }

    @Test
    public void testTruncate() throws Exception {
        StorageEngine<ByteArray, byte[], byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 });
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 });
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 });
        ByteArray key1 = new ByteArray((byte) 3);
        ByteArray key2 = new ByteArray((byte) 4);
        ByteArray key3 = new ByteArray((byte) 5);

        engine.put(key1, v1, null);
        engine.put(key2, v2, null);
        engine.put(key3, v3, null);
        engine.truncate();

        int count = testGetEntries(engine, null, new DefaultVoldemortFilter()).size();
        assertEquals("There shouldn't be any entries in this store.", 0, count);
    }

    /*
     * Note: unless the given StorageEngine overwrites the
     * getStorageEngine(String) method, this test will do nothing
     */
    @Test
    public void testMultipleStorageEngines() {
        StorageEngine<ByteArray, byte[], byte[]> engine1 = getStorageEngine("test1");
        StorageEngine<ByteArray, byte[], byte[]> engine2 = getStorageEngine("test2");
        if(!engine1.getName().equals(engine2.getName())) {
            Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(1));
            Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 }, TestUtils.getClock(3));
            Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(2));
            ByteArray key = new ByteArray((byte) 3);

            engine1.put(key, v1, null);
            engine1.put(key, v2, null);
            List<Versioned<byte[]>> r1 = engine1.get(key, null);
            List<Versioned<byte[]>> r2 = engine2.get(key, null);
            assertEquals(2, r1.size());
            assertEquals(0, r2.size());

            Version v = engine2.put(key, v3, null);
            r1 = engine1.get(key, null);
            r2 = engine2.get(key, null);
            assertEquals(1, r2.size());
            assertEquals(2, r1.size());
            assertTrue(TestUtils.bytesEqual(v3.getValue(), r2.get(0).getValue()));

            engine2.delete(key, v);
            r2 = engine2.get(key, null);
            r1 = engine1.get(key, null);
            assertEquals(0, r2.size());
            assertEquals(2, r1.size());
        }
    }

    @SuppressWarnings("unused")
    private boolean remove(List<byte[]> list, byte[] item) {
        Iterator<byte[]> it = list.iterator();
        boolean removedSomething = false;
        while(it.hasNext()) {
            if(TestUtils.bytesEqual(item, it.next())) {
                it.remove();
                removedSomething = true;
            }
        }
        return removedSomething;
    }

}
