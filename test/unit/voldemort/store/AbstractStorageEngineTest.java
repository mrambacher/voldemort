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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.serialization.StringSerializer;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableMap;

public abstract class AbstractStorageEngineTest extends AbstractByteArrayStoreTest {

    protected Map<String, StorageEngine<ByteArray, byte[]>> engines;

    public AbstractStorageEngineTest(String name) {
        super(name);
        engines = new HashMap<String, StorageEngine<ByteArray, byte[]>>();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        for(String name: engines.keySet()) {
            try {
                engines.get(name).close();
            } catch(Exception e) {}
        }
    }

    @Override
    public Store<ByteArray, byte[]> getStore(String name) {
        return getStorageEngine(name);
    }

    public StorageEngine<ByteArray, byte[]> getStorageEngine() {
        return getStorageEngine(storeName);
    }

    public StorageEngine<ByteArray, byte[]> getStorageEngine(String name) {
        StorageEngine<ByteArray, byte[]> engine = engines.get(name);
        if(engine == null) {
            engine = createStorageEngine(name);
            engines.put(name, engine);
        }
        return engine;
    }

    @Override
    protected void closeStore(String name) {
        StorageEngine<ByteArray, byte[]> engine = engines.get(name);
        if(engine != null) {
            engines.remove(name);
            engine.close();
        }
    }

    @Override
    public Store<ByteArray, byte[]> createStore(String name) {
        return getStorageEngine(name);
    }

    public abstract StorageEngine<ByteArray, byte[]> createStorageEngine(String name);

    @Test
    public void testGetNoEntries() {
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        try {
            StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
            it = engine.entries();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    @Test
    public void testHashCollisions() {

        ByteArray key1 = new ByteArray("40186".getBytes());
        ByteArray key2 = new ByteArray("797189".getBytes());
        HashFunction fnv = new FnvHashFunction();
        assertEquals("Hashes match", fnv.hash(key1.get()), fnv.hash(key2.get()));

        Store<ByteArray, byte[]> store = getStorageEngine();
        Versioned<byte[]> value1 = new Versioned<byte[]>(key1.get(), TestUtils.getClock(1));
        Versioned<byte[]> value2 = new Versioned<byte[]>(key2.get(), TestUtils.getClock(2));
        assertEquals("40186 value matches", testFetchedEqualsPut(store, key1, value1), 1);
        assertEquals("797189 value matches", testFetchedEqualsPut(store, key2, value2), 1);
    }

    @Test
    public void testOneKilobyteValueSizes() {
        testValueSizes("50-byte keys and with value size = 1024 bytes (1kb).", 1024, 50);
    }

    @Test
    public void testFiveKilobyteValueSizes() {
        testValueSizes("100-byte keys and with value size = 5*1024 bytes (5kb).", 5 * 1024, 100);
    }

    @Test
    public void testFiftyKilobyteValueSizes() {
        testValueSizes("150-byte keys and with value size = 50*1024 bytes (50kb).", 50 * 1024, 150);
    }

    @Test
    public void testFiveHundredKilobyteSizes() {
        testValueSizes("200-byte keys and with value size = 500*1024 bytes (1K).", 500 * 1024, 200);
    }

    private void testValueSizes(String valueSizeStr, int valueSize, int keySize) {

        format("Testing put()-get() call sequence with " + valueSizeStr, "", "");
        byte[] kNNNk = new byte[keySize];
        java.util.Arrays.fill(kNNNk, (byte) 163);

        byte[] valNNNk = new byte[valueSize];
        java.util.Arrays.fill(valNNNk, (byte) 59);

        Store<ByteArray, byte[]> store = getStorageEngine();

        ByteArray keyNNNk = new ByteArray(kNNNk);
        Versioned<byte[]> valueNNNk = new Versioned<byte[]>(valNNNk, TestUtils.getClock(1));

        Version verNNNk = store.put(keyNNNk, valueNNNk);
        List<Versioned<byte[]>> rNNNk = store.get(keyNNNk);
        Version rverNNNk = rNNNk.get(0).getVersion();
        assertTrue(verNNNk.compare(rverNNNk) == rverNNNk.compare(verNNNk));
        assertTrue(TestUtils.bytesEqual(valueNNNk.getValue(), rNNNk.get(0).getValue()));

    }

    @Test
    public void testGetNoKeys() {
        ClosableIterator<ByteArray> it = null;
        try {
            StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
            it = engine.keys();
            while(it.hasNext())
                fail("There shouldn't be any entries in this store.");
        } finally {
            if(it != null)
                it.close();
        }
    }

    @Test
    public void testKeyIterationWithSerialization() {
        StorageEngine<ByteArray, byte[]> store = getStorageEngine();
        StorageEngine<String, String> stringStore = new SerializingStorageEngine<String, String>(store,
                                                                                                 new StringSerializer(),
                                                                                                 new StringSerializer());
        Map<String, String> vals = ImmutableMap.of("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
        for(Map.Entry<String, String> entry: vals.entrySet())
            stringStore.put(entry.getKey(), new Versioned<String>(entry.getValue()));
        ClosableIterator<String> iter = stringStore.keys();
        int count = 0;
        while(iter.hasNext()) {
            String key = iter.next();
            assertTrue(vals.containsKey(key));
            count++;
        }
        assertEquals(count, vals.size());
        iter.close();
    }

    @Test
    public void testIterationWithSerialization() {
        StorageEngine<ByteArray, byte[]> store = getStorageEngine();
        StorageEngine<String, String> stringStore = SerializingStorageEngine.wrap(store,
                                                                                  new StringSerializer(),
                                                                                  new StringSerializer());
        Map<String, String> vals = ImmutableMap.of("a", "a", "b", "b", "c", "c", "d", "d", "e", "e");
        for(Map.Entry<String, String> entry: vals.entrySet())
            stringStore.put(entry.getKey(), new Versioned<String>(entry.getValue()));
        ClosableIterator<Pair<String, Versioned<String>>> iter = stringStore.entries();
        int count = 0;
        while(iter.hasNext()) {
            Pair<String, Versioned<String>> keyAndVal = iter.next();
            assertTrue(vals.containsKey(keyAndVal.getFirst()));
            assertEquals(vals.get(keyAndVal.getFirst()), keyAndVal.getSecond().getValue());
            count++;
        }
        assertEquals(count, vals.size());
        iter.close();
    }

    @Test
    public void testPruneOnWrite() {
        StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(1));
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 }, TestUtils.getClock(2));
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 }, TestUtils.getClock(1, 2));
        ByteArray key = new ByteArray((byte) 3);
        engine.put(key, v1);
        engine.put(key, v2);
        assertEquals(2, engine.get(key).size());
        engine.put(key, v3);
        assertEquals(1, engine.get(key).size());
    }

    @Test
    public void testTruncate() throws Exception {
        StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 });
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 });
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 });
        ByteArray key1 = new ByteArray((byte) 3);
        ByteArray key2 = new ByteArray((byte) 4);
        ByteArray key3 = new ByteArray((byte) 5);

        engine.put(key1, v1);
        engine.put(key2, v2);
        engine.put(key3, v3);
        engine.truncate();

        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> it = null;
        try {
            it = engine.entries();
            while(it.hasNext()) {
                fail("There shouldn't be any entries in this store.");
            }
        } finally {
            if(it != null) {
                it.close();
            }
        }
    }

    /*
     * Note: unless the given StorageEngine overwrites the
     * getStorageEngine(String) method, this test will do nothing
     */
    @Test
    public void testMultipleStorageEngines() {
        StorageEngine<ByteArray, byte[]> engine1 = getStorageEngine("test1");
        StorageEngine<ByteArray, byte[]> engine2 = getStorageEngine("test2");
        if(!engine1.getName().equals(engine2.getName())) {
            Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(1));
            Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 }, TestUtils.getClock(3));
            Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(2));
            ByteArray key = new ByteArray((byte) 3);

            engine1.put(key, v1);
            engine1.put(key, v2);
            List<Versioned<byte[]>> r1 = engine1.get(key);
            List<Versioned<byte[]>> r2 = engine2.get(key);
            assertEquals(2, r1.size());
            assertEquals(0, r2.size());

            Version v = engine2.put(key, v3);
            r1 = engine1.get(key);
            r2 = engine2.get(key);
            assertEquals(1, r2.size());
            assertEquals(2, r1.size());
            assertTrue(TestUtils.bytesEqual(v3.getValue(), r2.get(0).getValue()));

            engine2.delete(key, v);
            r2 = engine2.get(key);
            r1 = engine1.get(key);
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
