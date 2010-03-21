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

import java.util.List;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * 
 */
public abstract class AbstractByteArrayStoreTest extends AbstractStoreTest<ByteArray, byte[]> {

    public AbstractByteArrayStoreTest(String name) {
        super(name);
    }

    @Override
    public List<ByteArray> getKeys(int numValues) {
        List<ByteArray> keys = Lists.newArrayList();
        for(byte[] array: this.getByteValues(numValues, 8))
            keys.add(new ByteArray(array));
        return keys;
    }

    @Override
    public List<byte[]> getValues(int numValues) {
        return this.getByteValues(numValues, 10);
    }

    @Override
    protected boolean valuesEqual(byte[] t1, byte[] t2) {
        return TestUtils.bytesEqual(t1, t2);
    }

    @Test
    public void testEmptyByteArray() throws Exception {
        Store<ByteArray, byte[]> store = getStore();
        Versioned<byte[]> bytes = new Versioned<byte[]>(new byte[0]);
        store.put(new ByteArray(new byte[0]), bytes);
        List<Versioned<byte[]>> found = store.get(new ByteArray(new byte[0]));
        assertEquals("Incorrect number of results.", 1, found.size());
        assertEquals("Get doesn't equal put.", bytes, found.get(0));
    }

    @Test
    public void testHashCollisions() {

        ByteArray key1 = new ByteArray("40186".getBytes());
        ByteArray key2 = new ByteArray("797189".getBytes());
        HashFunction fnv = new FnvHashFunction();
        assertEquals("Hashes match", fnv.hash(key1.get()), fnv.hash(key2.get()));

        Store<ByteArray, byte[]> store = getStore();
        Versioned<byte[]> value1 = new Versioned<byte[]>(key1.get(), TestUtils.getClock(1));
        Versioned<byte[]> value2 = new Versioned<byte[]>(key2.get(), TestUtils.getClock(2));
        assertEquals("40186 value matches", testFetchedEqualsPut(store, key1, value1), 1);
        assertEquals("797189 value matches", testFetchedEqualsPut(store, key2, value2), 1);
    }

    @Test
    public void testOneKilobyteValueSizes() {
        testValueSizes("50-byte keys and with value size = 1024 bytes (1kb).", 1024, 50, null);
    }

    @Test
    public void testFiveKilobyteValueSizes() {
        testValueSizes("100-byte keys and with value size = 5*1024 bytes (5kb).",
                       5 * 1024,
                       100,
                       null);
    }

    @Test
    public void testFiftyKilobyteValueSizes() {
        testValueSizes("150-byte keys and with value size = 50*1024 bytes (50kb).",
                       50 * 1024,
                       150,
                       null);
    }

    @Test
    public void testFiveHundredKilobyteSizes() {
        testValueSizes("200-byte keys and with value size = 500*1024 bytes (1K).",
                       500 * 1024,
                       200,
                       null);
    }

    @Test
    public void testSixtyMegabyteSizes() {
        testValueSizes("250-byte keys and with value size = 60*1024*1024 bytes (60 MB).",
                       60 * 1024 * 1024,
                       250,
                       null);
    }

    @SuppressWarnings("unused")
    protected boolean supportsSizes(int valueSize, int keySize) {
        return true;
    }

    protected void testValueSizes(String valueSizeStr,
                                  int valueSize,
                                  int keySize,
                                  Class<? extends Exception> exception) {
        if(supportsSizes(keySize, valueSize)) {
            try {
                format("Testing put()-get() call sequence with " + valueSizeStr, "", "");
                byte[] kNNNk = new byte[keySize];
                java.util.Arrays.fill(kNNNk, (byte) 163);

                byte[] valNNNk = new byte[valueSize];
                java.util.Arrays.fill(valNNNk, (byte) 59);

                Store<ByteArray, byte[]> store = getStore();

                ByteArray keyNNNk = new ByteArray(kNNNk);
                Versioned<byte[]> valueNNNk = new Versioned<byte[]>(valNNNk, TestUtils.getClock(1));

                Version verNNNk = store.put(keyNNNk, valueNNNk);
                List<Versioned<byte[]>> rNNNk = store.get(keyNNNk);
                Version rverNNNk = rNNNk.get(0).getVersion();
                assertTrue(verNNNk.compare(rverNNNk) == rverNNNk.compare(verNNNk));
                assertTrue(TestUtils.bytesEqual(valueNNNk.getValue(), rNNNk.get(0).getValue()));
            } catch(Exception e) {
                assertEquals("Unexpected exception " + e.getClass().getName(),
                             exception,
                             e.getClass());
            }
        }
    }

    @Test
    public void testPruneOnWrite() {
        Store<ByteArray, byte[]> engine = getStore();
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
}
