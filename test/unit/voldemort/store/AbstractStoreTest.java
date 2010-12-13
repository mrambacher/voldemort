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

import static voldemort.TestUtils.getClock;
import static voldemort.TestUtils.randomLetters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

public abstract class AbstractStoreTest<K, V, T> extends AbstractVoldemortTest<V> {

    protected Map<String, Store<K, V, T>> stores;

    protected String storeName;

    protected AbstractStoreTest(String name) {
        this.storeName = name;
        stores = new HashMap<String, Store<K, V, T>>();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        for(String name: stores.keySet()) {
            closeStore(name);
        }
    }

    protected void closeStore(String name) {
        try {
            Store<K, V, T> store = stores.get(name);
            if(store != null) {
                stores.remove(name);
                store.close();
            }
        } catch(Exception e) {}
    }

    protected Store<K, V, T> getStore() {
        return getStore(storeName);
    }

    public Store<K, V, T> getStore(String name) {
        Store<K, V, T> store = stores.get(name);
        if(store == null) {
            store = createStore(name);
            stores.put(name, store);
        }
        return store;
    }

    protected Version doPut(K key, V value) {
        return doPut(key, new Versioned<V>(value));
    }

    protected Version doPut(K key, Versioned<V> value) {
        return doPut(this.storeName, key, value);
    }

    protected Version doPut(String name, K key, Versioned<V> value) {
        return doPut(name, key, value, null);
    }

    protected Version doPut(String name, K key, Versioned<V> value, T transform) {
        Store<K, V, T> store = getStore(name);
        return store.put(key, value, transform);
    }

    protected List<Versioned<V>> doGet(K key) {
        return doGet(storeName, key);
    }

    protected List<Versioned<V>> doGet(String name, K key) {
        return doGet(name, key, null);
    }

    protected List<Versioned<V>> doGet(String name, K key, T transform) {
        Store<K, V, T> store = getStore(name);
        return store.get(key, transform);
    }

    protected List<Version> doGetVersions(K key) {
        return doGetVersions(storeName, key);
    }

    protected List<Version> doGetVersions(String name, K key) {
        Store<K, V, T> store = getStore(name);
        return store.getVersions(key);
    }

    protected Map<K, List<Versioned<V>>> doGetAll(Iterable<K> keys) {
        return doGetAll(storeName, keys);
    }

    protected Map<K, List<Versioned<V>>> doGetAll(String name, Iterable<K> key) {
        return doGetAll(name, key, null);
    }

    protected Map<K, List<Versioned<V>>> doGetAll(String name, Iterable<K> key, Map<K, T> transforms) {
        Store<K, V, T> store = getStore(name);
        return store.getAll(key, transforms);
    }

    protected boolean doDelete(K key) {
        return doDelete(key, VersionFactory.newVersion());
    }

    protected boolean doDelete(K key, Version version) {
        return doDelete(this.storeName, key, version);
    }

    protected boolean doDelete(String name, K key, Version version) {
        Store<K, V, T> store = getStore(name);
        return store.delete(key, version);
    }

    protected void doClose() {
        doClose(storeName);
    }

    protected void doClose(String name) {
        Store<K, V, T> store = getStore(name);
        store.close();
    }

    public abstract Store<K, V, T> createStore(String name);

    public abstract List<V> getValues(int numValues);

    public abstract List<K> getKeys(int numKeys);

    public List<String> getStrings(int numKeys, int size) {
        List<String> ts = new ArrayList<String>(numKeys);
        for(int i = 0; i < numKeys; i++)
            ts.add(randomLetters(size));
        return ts;
    }

    public List<byte[]> getByteValues(int numValues, int size) {
        List<byte[]> values = new ArrayList<byte[]>();
        for(int i = 0; i < numValues; i++)
            values.add(TestUtils.randomBytes(size));
        return values;
    }

    public List<ByteArray> getByteArrayValues(int numValues, int size) {
        List<ByteArray> values = new ArrayList<ByteArray>();
        for(int i = 0; i < numValues; i++)
            values.add(new ByteArray(TestUtils.randomBytes(size)));
        return values;
    }

    public K getKey() {
        return getKeys(1).get(0);
    }

    public V getValue() {
        return getValues(1).get(0);
    }

    public Version getExpectedVersionAfterPut(Version version) {
        return version;
    }

    @Test
    public void testNullKeys() throws Exception {
        try {
            doPut(null, new Versioned<V>(getValue()));
            fail("Store should not put null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            doGet(null);
            fail("Store should not get null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            doGetAll(null);
            fail("Store should not getAll null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            doGetAll(Collections.<K> singleton(null));
            fail("Store should not getAll null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            doDelete(null, new VectorClock());
            fail("Store should not delete null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        }
    }

    @Test
    public void testPutNullValue() {
    // Store<K,V> store = getStore();
    // K key = getKey();
    // store.put(key, new Versioned<V>(null));
    // List<Versioned<V>> found = store.get(key);
    // assertEquals("Wrong number of values.", 1, found.size());
    // assertEquals("Returned non-null value.", null, found.get(0).getValue());
    }

    @Test
    public void testGetAndDeleteNonExistentKey() throws Exception {
        K key = getKey();
        List<Versioned<V>> found = doGet(key);
        assertEquals("Found non-existent key: " + found, 0, found.size());
        assertTrue("Delete of non-existent key succeeded.", !doDelete(key, getClock(1,
                                                                                    1,
                                                                                    2,
                                                                                    2,
                                                                                    3,
                                                                                    3)));
    }

    protected Version checkForObsoleteVersion(Version clock,
                                              int count,
                                              K key,
                                              Versioned<V> versioned,
                                              VoldemortException e) {
        assertEquals(ObsoleteVersionException.class, e.getClass());
        if(e instanceof ObsoleteVersionException) {
            ObsoleteVersionException ove = (ObsoleteVersionException) e;
            // this is good, but check that we didn't fuck with the version
            assertEquals(clock, versioned.getVersion());
            assertEquals(count, doGet(storeName, key).size()); // Make sure we
            // did not
            // delete anything by mistake
            return ove.getExistingVersion();
        }
        return null;
    }

    private Version testObsoletePutFails(String message,
                                         String storeName,
                                         K key,
                                         Versioned<V> versioned) {
        VectorClock clock = (VectorClock) versioned.getVersion();
        int count = doGet(storeName, key).size();
        clock = clock.clone();
        try {
            doPut(storeName, key, versioned);
            fail(message);
        } catch(VoldemortException e) {
            return checkForObsoleteVersion(clock, count, key, versioned, e);
        }
        return null;
    }

    protected int testFetchedEqualsPut(K key, Versioned<V> put) {
        return testFetchedEqualsPut(storeName, key, put);
    }

    @SuppressWarnings("unused")
    protected List<Versioned<V>> assertFetchedEqualsPut(String storeName,
                                                        K key,
                                                        Version version,
                                                        Versioned<V> put,
                                                        List<Versioned<V>> results) {
        boolean found = false;
        Versioned<V> versioned = new Versioned<V>(put.getValue(), version, put.getMetadata());
        for(Versioned<V> result: results) {
            if(version.equals(result.getVersion())) {
                found = true;
                assertTrue("Values not equal!",
                           valuesEqual(versioned.getValue(), result.getValue()));
                assertEquals("Versioneds not equal.", versioned, result);
                break;
            }
        }
        if(!found) {
            fail("Saved version not retrieved");
        }
        return results;
    }

    protected int testFetchedEqualsPut(String name, K key, Versioned<V> put) {
        Version version = doPut(name, key, put);
        List<Versioned<V>> results = assertFetchedEqualsPut(name, key, version, put, doGet(name,
                                                                                           key));
        return results.size();

    }

    @Test
    public void testFetchedEqualsPut() throws Exception {
        K key = getKey();
        Version version = TestUtils.getClock(1, 1, 2, 3, 3, 4);
        V value = getValue();
        assertEquals("Store not empty at start!", 0, doGet(key).size());
        Versioned<V> versioned = new Versioned<V>(value, version);
        int count = testFetchedEqualsPut(key, versioned);
        assertEquals("Should only be one version stored.", 1, count);
    }

    @Test
    public void testVersionedPut() throws Exception {
        K key = getKey();
        Version clock = getClock(1, 1);
        Version clockCopy = VersionFactory.cloneVersion(clock);
        V value = getValue();
        assertEquals("Store not empty at start!", 0, doGet(key).size());
        Versioned<V> versioned = new Versioned<V>(value, clock);

        // put initial version
        doPut(key, versioned);
        assertContains(doGet(key), versioned);

        // test that putting obsolete versions fails
        testObsoletePutFails("Put of identical version/value succeeded.",
                             storeName,
                             key,
                             new Versioned<V>(value, clockCopy));
        testObsoletePutFails("Put of identical version succeeded.",
                             storeName,
                             key,
                             new Versioned<V>(getValue(), clockCopy));
        testObsoletePutFails("Put of obsolete version succeeded.",
                             storeName,
                             key,
                             new Versioned<V>(getValue(), getClock(1)));
        assertEquals("Should still only be one version in store.", doGet(key).size(), 1);
        assertContains(doGet(key), versioned);

        // test that putting a concurrent version succeeds
        if(allowConcurrentOperations()) {
            doPut(key, new Versioned<V>(getValue(), getClock(1, 2)));
            assertEquals(2, doGet(key).size());
        } else {
            try {
                doPut(key, new Versioned<V>(getValue(), getClock(1, 2)));
                fail();
            } catch(ObsoleteVersionException e) {
                // expected
            }
        }

        // test that putting an incremented version succeeds
        Versioned<V> newest = new Versioned<V>(getValue(), getClock(1, 1, 2, 2));
        doPut(key, newest);
        assertContains(doGet(key), newest);
    }

    protected boolean supportsMetadata() {
        return true;
    }

    @Test
    public void testMetadata() {
        if(supportsMetadata()) {
            K key = getKey();
            Version version = TestUtils.getClock(1, 1, 2, 3, 3, 4);
            V value = getValue();
            assertEquals("Store not empty at start!", 0, doGet(key).size());
            Versioned<V> versioned = new Versioned<V>(value, version);
            versioned.getMetadata().setProperty("test", "metadata");
            int count = testFetchedEqualsPut(storeName, key, versioned);
            assertEquals("Should only be one version stored.", 1, count);
        }
    }

    @Test
    public void testDelete() throws Exception {
        K key = getKey();
        Version c1 = getClock(1, 1);
        Version c2 = getClock(1, 2);
        V value = getValue();

        // can't delete something that isn't there
        assertTrue(!doDelete(key, c1));

        // put two conflicting versions, then delete one
        Versioned<V> v1 = new Versioned<V>(value, c1);
        Versioned<V> v2 = new Versioned<V>(value, c2);
        Version m1 = doPut(key, v1);
        Version m2 = doPut(key, v2);
        assertTrue("Delete failed!", doDelete(key, m1));

        List<Versioned<V>> found = doGet(key);
        assertContainsVersioned("One version after delete", new Versioned<V>(value, m2), found);

        // now delete that version too
        assertTrue("Delete failed!", doDelete(key, m2));
        assertEquals(0, doGet(key).size());
    }

    @Test
    public void testGetVersions() throws Exception {
        List<K> keys = getKeys(2);
        K key = keys.get(0);
        V value = getValue();
        Version version = doPut(key, Versioned.value(value));
        List<Version> versions = doGetVersions(key);

        assertTrue(versions.size() > 0);
        for(int i = 0; i < versions.size(); i++)
            assertEquals(version, versions.get(i));

        assertEquals(0, doGetVersions(keys.get(1)).size());
    }

    @Test
    public void testGetAll() throws Exception {
        int putCount = 10;
        List<K> keys = getKeys(putCount);
        List<V> values = getValues(putCount);
        assertEquals(putCount, values.size());

        for(int i = 0; i < putCount; i++) {
            doPut(keys.get(i), new Versioned<V>(values.get(i)));
        }
        int countForGet = putCount / 2;
        List<K> keysForGet = keys.subList(0, countForGet);
        List<V> valuesForGet = values.subList(0, countForGet);
        Map<K, List<Versioned<V>>> result = doGetAll(keysForGet);
        assertEquals(countForGet, result.size());
        for(int i = 0; i < keysForGet.size(); ++i) {
            K key = keysForGet.get(i);
            V expectedValue = valuesForGet.get(i);
            List<Versioned<V>> versioneds = result.get(key);
            assertGetAllValues(expectedValue, versioneds);
        }
    }

    @Test
    public void testGetAllWithAbsentKeys() throws Exception {
        Map<K, List<Versioned<V>>> result = doGetAll(getKeys(3));
        assertEquals(0, result.size());
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        doClose(storeName);
        // second close is okay, should not throw an exception
        doClose(storeName);
    }

    protected void assertGetAllValues(V expectedValue, List<Versioned<V>> versioneds) {
        assertEquals(1, versioneds.size());
        valuesEqual(expectedValue, versioneds.get(0).getValue());
    }

    protected boolean allowConcurrentOperations() {
        return true;
    }
}
