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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileDeleteStrategy;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseException;

public class BdbStorageEngineTest extends AbstractStorageEngineTest {

    private BdbConfiguration configuration;

    public BdbStorageEngineTest() {
        super("test");
    }

    private File tempDir;
    private StorageEngine<ByteArray, byte[]> store;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.tempDir = TestUtils.createTempDir();
        configuration = new BdbConfiguration(tempDir, "test");
        this.store = getStorageEngine("test");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            configuration.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(tempDir);
        }
    }

    @Override
    public BdbStorageEngine createStorageEngine(String name) {
        try {
            return configuration.createStorageEngine(name);
        } catch(DatabaseException e) {
            assertNull("Unexpected exception", e);
            return null;
        }
    }

    @Test
    public void testPersistence() throws Exception {
        StorageEngine<ByteArray, byte[]> store = this.getStorageEngine();

        store.put(new ByteArray("abc".getBytes()), new Versioned<byte[]>("cdef".getBytes()));
        this.closeStore(store.getName());
        this.configuration.close();
        this.configuration = new BdbConfiguration(this.tempDir, "test");
        store = getStorageEngine();
        List<Versioned<byte[]>> vals = store.get(new ByteArray("abc".getBytes()));
        assertEquals(1, vals.size());
        TestUtils.bytesEqual("cdef".getBytes(), vals.get(0).getValue());
    }

    @Test
    public void testEquals() {
        String name = "someName";
        BdbStorageEngine first = createStorageEngine(name);
        BdbStorageEngine second = createStorageEngine(name);
        assertEquals(first, second);
        first.close();
        second.close();
    }

    @Test
    public void testNullConstructorParameters() {
        try {
            createStorageEngine(null);
            fail("No exception thrown for null name.");
        } catch(IllegalArgumentException e) {
            return;
        }
    }

    @Test
    public void testSimultaneousIterationAndModification() throws Exception {
        // start a thread to do modifications
        ExecutorService executor = Executors.newFixedThreadPool(2);
        final Random rand = new Random();
        final AtomicInteger count = new AtomicInteger(0);
        executor.execute(new Runnable() {

            public void run() {
                while(!Thread.interrupted()) {
                    byte[] bytes = Integer.toString(count.getAndIncrement()).getBytes();
                    store.put(new ByteArray(bytes), Versioned.value(bytes));
                    count.incrementAndGet();
                }
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                while(!Thread.interrupted()) {
                    byte[] bytes = Integer.toString(rand.nextInt(count.get())).getBytes();
                    store.delete(new ByteArray(bytes), new VectorClock());
                    count.incrementAndGet();
                }
            }
        });

        // wait a bit
        while(count.get() < 300)
            continue;

        // now simultaneously do iteration
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter = this.store.entries();
        while(iter.hasNext())
            iter.next();
        iter.close();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
    }
}
