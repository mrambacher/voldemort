/*
 * Copyright 2010 Nokia Corporation. All rights reserved.
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
package voldemort.store.async;

import static voldemort.TestUtils.getClock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import voldemort.VoldemortException;
import voldemort.client.VoldemortInterruptedException;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

abstract public class AbstractAsynchronousStoreTest extends AbstractByteArrayStoreTest {

    protected Map<String, AsynchronousStore<ByteArray, byte[]>> asyncStores;

    protected AbstractAsynchronousStoreTest(String name) {
        super(name);
        asyncStores = new HashMap<String, AsynchronousStore<ByteArray, byte[]>>();
    }

    public AsynchronousStore<ByteArray, byte[]> getAsyncStore(String name) {
        AsynchronousStore<ByteArray, byte[]> async = asyncStores.get(name);
        if(async == null) {
            async = createAsyncStore(name);
            asyncStores.put(name, async);
        }
        return async;
    }

    protected AsynchronousStore<ByteArray, byte[]> getSlowStore(String name) {
        return createSlowStore(name, 2000);
    }

    abstract protected AsynchronousStore<ByteArray, byte[]> createSlowStore(String name, long delay);

    abstract protected AsynchronousStore<ByteArray, byte[]> createFailingStore(String name,
                                                                               VoldemortException ex);

    public abstract AsynchronousStore<ByteArray, byte[]> createAsyncStore(String name);

    @Override
    public Store<ByteArray, byte[]> createStore(String name) {
        fail("Unexpected API Invocation");
        return null;
    }

    protected <R> R waitForCompletion(StoreFuture<R> future) throws VoldemortException {
        return future.get();
    }

    @Override
    protected Version doPut(String name, ByteArray key, Versioned<byte[]> value) {
        AsynchronousStore<ByteArray, byte[]> store = getAsyncStore(name);
        return waitForCompletion(store.submitPut(key, value));
    }

    @Override
    protected List<Versioned<byte[]>> doGet(String name, ByteArray key) {
        AsynchronousStore<ByteArray, byte[]> store = getAsyncStore(name);
        return waitForCompletion(store.submitGet(key));
    }

    @Override
    protected Map<ByteArray, List<Versioned<byte[]>>> doGetAll(String name, Iterable<ByteArray> keys) {
        AsynchronousStore<ByteArray, byte[]> store = getAsyncStore(name);
        return waitForCompletion(store.submitGetAll(keys));
    }

    @Override
    protected boolean doDelete(String name, ByteArray key, Version version) {
        AsynchronousStore<ByteArray, byte[]> store = getAsyncStore(name);
        return waitForCompletion(store.submitDelete(key, version));
    }

    @Override
    protected List<Version> doGetVersions(String name, ByteArray key) {
        AsynchronousStore<ByteArray, byte[]> store = getAsyncStore(name);
        return waitForCompletion(store.submitGetVersions(key));
    }

    @Override
    protected void doClose(String name) {
        AsynchronousStore<ByteArray, byte[]> store = getAsyncStore(name);
        try {
            store.close();
        } catch(VoldemortException e) {
            if(!e.getMessage().equals("oops")) {
                throw e;
            }
        }
    }

    protected <R> void testInterruptedFuture(StoreFuture<R> future) {
        future.cancel(true);
        try {
            future.get();
            fail("Expected interrupted exception");
        } catch(Exception e) {
            assertEquals("Unexpected exception", VoldemortInterruptedException.class, e.getClass());
        }
    }

    protected <R> void testFutureTimeout(long timeout, StoreFuture<R> future) {
        testSlowFuture(timeout, future, UnreachableStoreException.class);
    }

    protected <R> void testFutureTimeout(long timeout, long delay, StoreFuture<R> future) {
        long left = delay;
        do {
            testFutureTimeout(timeout, future);
            left -= timeout;
        } while(left > timeout);
        testSlowFuture(timeout, future, null);
    }

    protected <R> void testSlowFuture(long timeout,
                                      StoreFuture<R> future,
                                      Class<? extends VoldemortException> exception) {
        try {
            R result = future.get(timeout, TimeUnit.MILLISECONDS);
            if(exception != null) {
                fail("Expected timeout exception");
            }
        } catch(Exception e) {
            assertEquals("Unexpected exception", exception, e.getClass());
        }
    }

    @Test
    public void testInterruptedFuture() {
        AsynchronousStore<ByteArray, byte[]> store = getSlowStore("slow");
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        testInterruptedFuture(store.submitGet(key));
        testInterruptedFuture(store.submitGetVersions(key));
        testInterruptedFuture(store.submitGetAll(keys));
        testInterruptedFuture(store.submitPut(key, new Versioned<byte[]>(value)));
        testInterruptedFuture(store.submitDelete(key, VersionFactory.newVersion()));
    }

    @Test
    public void testFutureTimeouts() {
        long delay = 500;
        Maps.newHashMap();
        AsynchronousStore<ByteArray, byte[]> store = createSlowStore("sleepy", delay);
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        testFutureTimeout(400, delay, store.submitGet(key));
        testFutureTimeout(400, delay, store.submitGetVersions(key));
        testFutureTimeout(400, delay, store.submitGetAll(keys));
        testFutureTimeout(400, delay, store.submitPut(key, new Versioned<byte[]>(value)));
        testFutureTimeout(400, delay, store.submitDelete(key, VersionFactory.newVersion()));
    }

    @Test
    public void testTimedFutures() {
        AsynchronousStore<ByteArray, byte[]> store = createSlowStore("timed", 100);
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        testSlowFuture(200, store.submitGet(key), null);
        testSlowFuture(200, store.submitGetVersions(key), null);
        testSlowFuture(200, store.submitGetAll(keys), null);
        testSlowFuture(200, store.submitPut(key, new Versioned<byte[]>(value)), null);
        testSlowFuture(200, store.submitDelete(key, VersionFactory.newVersion()), null);
    }

    @Test
    public void testFailingFutures() {
        AsynchronousStore<ByteArray, byte[]> store = createSlowStore(storeName, 100);
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        testSlowFuture(200, store.submitGet(key), null);
        testSlowFuture(200, store.submitGetVersions(key), null);
        testSlowFuture(200, store.submitGetAll(keys), null);
        testSlowFuture(200, store.submitPut(key, new Versioned<byte[]>(value)), null);
        testSlowFuture(200, store.submitDelete(key, VersionFactory.newVersion()), null);
    }

    protected void checkListenerException(final Class<? extends VoldemortException> expected,
                                          VoldemortException result) {
        assertEquals("Unexcepted exception ", expected, result.getClass());
    }

    protected <R> void registerCountedListener(Integer index,
                                               StoreFuture<R> future,
                                               final long delay,
                                               final boolean failing,
                                               final Class<? extends VoldemortException> expected,
                                               final AtomicInteger finished) {
        future.register(new StoreFutureListener<R>() {

            public void futureCompleted(Object index, R result, long duration) {
                if(expected != null) {
                    fail(index + "Excepted exception " + expected);
                }
                if(delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch(Exception e) {
                        // Eat it
                    }
                }
                finished.decrementAndGet();
                if(failing) {
                    throw new VoldemortException("Future failed");
                }
            }

            public void futureFailed(Object index, VoldemortException ex, long duration) {
                // checkListenerException(expected, ex);
                if(delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch(Exception e) {
                        // Eat it
                    }
                }
                finished.decrementAndGet();
                if(failing) {
                    throw new VoldemortException("Future failed");
                }
            }
        }, future.getOperation() + "_" + index);
    }

    protected void testFutureListeners(List<StoreFuture<?>> futures, AtomicInteger finished) {
        for(StoreFuture<?> future: futures) {
            try {
                future.get();
            } catch(VoldemortException e) {
                // fail("Future get failed: " + e.getMessage());
            }
        }
        assertEquals("Completed equals done", 0, finished.intValue());
    }

    protected AtomicInteger registerCountedListeners(List<StoreFuture<?>> futures, long delay) {
        return registerCountedListeners(futures, delay, null);
    }

    protected AtomicInteger registerCountedListeners(List<StoreFuture<?>> futures,
                                                     long delay,
                                                     Class<? extends VoldemortException> exception) {
        return registerCountedListeners(futures, delay, false, exception);
    }

    protected AtomicInteger registerCountedListeners(List<StoreFuture<?>> futures,
                                                     long delay,
                                                     boolean throwEx,
                                                     Class<? extends VoldemortException> exception) {

        final AtomicInteger finished = new AtomicInteger(futures.size());
        for(int i = 0; i < futures.size(); i++) {
            this.registerCountedListener(i, futures.get(i), delay, throwEx, exception, finished);
        }
        return finished;
    }

    @Test
    public void testFutureListeners() {
        AsynchronousStore<ByteArray, byte[]> store = getSlowStore("slow");
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        List<StoreFuture<?>> futures = new ArrayList<StoreFuture<?>>();
        futures.add(store.submitGet(key));
        futures.add(store.submitGetAll(keys));
        futures.add(store.submitPut(key, new Versioned<byte[]>(value)));
        futures.add(store.submitDelete(key, VersionFactory.newVersion()));
        futures.add(store.submitGetVersions(key));
        final AtomicInteger finished = registerCountedListeners(futures, 0);
        testFutureListeners(futures, finished);
    }

    @Test
    public void testFutureFailureListeners() {
        AsynchronousStore<ByteArray, byte[]> store = createFailingStore("faililng",
                                                                        new VoldemortException("oops"));
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        List<StoreFuture<?>> futures = new ArrayList<StoreFuture<?>>();
        futures.add(store.submitGet(key));
        futures.add(store.submitGetAll(keys));
        futures.add(store.submitPut(key, new Versioned<byte[]>(value)));
        futures.add(store.submitDelete(key, VersionFactory.newVersion()));
        futures.add(store.submitGetVersions(key));
        final AtomicInteger finished = registerCountedListeners(futures,
                                                                0,
                                                                VoldemortException.class);
        testFutureListeners(futures, finished);
    }

    @Test
    public void testSlowListeners() {
        AsynchronousStore<ByteArray, byte[]> store = getSlowStore("slow");
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        List<StoreFuture<?>> futures = new ArrayList<StoreFuture<?>>();
        futures.add(store.submitGet(key));
        futures.add(store.submitGetAll(keys));
        futures.add(store.submitPut(key, new Versioned<byte[]>(value)));
        futures.add(store.submitDelete(key, VersionFactory.newVersion()));
        futures.add(store.submitGetVersions(key));
        final AtomicInteger finished = registerCountedListeners(futures, 100);

        testFutureListeners(futures, finished);
    }

    protected AtomicInteger registerFailingListeners(List<StoreFuture<?>> futures,
                                                     long delay,
                                                     Class<? extends VoldemortException> exception) {
        return this.registerCountedListeners(futures, delay, true, exception);
    }

    @Test
    public void testFailingListeners() {
        AsynchronousStore<ByteArray, byte[]> store = getSlowStore("slow");
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        List<StoreFuture<?>> futures = new ArrayList<StoreFuture<?>>();
        futures.add(store.submitGet(key));
        futures.add(store.submitGetAll(keys));
        futures.add(store.submitPut(key, new Versioned<byte[]>(value)));
        futures.add(store.submitDelete(key, VersionFactory.newVersion()));
        futures.add(store.submitGetVersions(key));
        final AtomicInteger finished = registerFailingListeners(futures, 0, null);

        testFutureListeners(futures, finished);
    }

    @Test
    public void testTwoListeners() {
        AsynchronousStore<ByteArray, byte[]> store = getSlowStore("slow");
        ByteArray key = getKey();
        byte[] value = getValue();
        List<ByteArray> keys = getKeys(5);
        List<StoreFuture<?>> futures = new ArrayList<StoreFuture<?>>();
        futures.add(store.submitGet(key));
        futures.add(store.submitGetAll(keys));
        futures.add(store.submitPut(key, new Versioned<byte[]>(value)));
        futures.add(store.submitDelete(key, VersionFactory.newVersion()));
        futures.add(store.submitGetVersions(key));
        final AtomicInteger finished1 = registerCountedListeners(futures, 0);
        final AtomicInteger finished2 = registerCountedListeners(futures, 0);

        for(StoreFuture<?> future: futures) {
            try {
                future.get();
            } catch(VoldemortException e) {

            }
        }
        assertEquals("Completed equals done", 0, finished1.intValue());
        assertEquals("Completed equals done", 0, finished2.intValue());
    }

    @Test
    public void testFutureIsIdempotent() {
        AsynchronousStore<ByteArray, byte[]> store = getSlowStore("slow");
        byte[] value = getValue();
        ByteArray key = getKey();
        AtomicInteger futureCount = new AtomicInteger(4);
        StoreFuture<Version> put = store.submitPut(key,
                                                   new Versioned<byte[]>(value, getClock(1, 1)));
        this.registerCountedListener(0, put, 0, false, null, futureCount);
        Version version = this.waitForCompletion(put);
        Versioned<byte[]> expected = new Versioned<byte[]>(value, version);

        StoreFuture<List<Versioned<byte[]>>> get = store.submitGet(key);
        this.registerCountedListener(0, get, 0, false, null, futureCount);
        assertContainsVersioned("Version found", expected, waitForCompletion(get));
        StoreFuture<Map<ByteArray, List<Versioned<byte[]>>>> getAll = store.submitGetAll(Collections.singleton(key));

        this.registerCountedListener(0, getAll, 0, false, null, futureCount);
        Map<ByteArray, List<Versioned<byte[]>>> all = waitForCompletion(getAll);
        assertEquals("One key", 1, all.size());
        this.assertGetAllValues(value, all.get(key));

        StoreFuture<Boolean> delete = store.submitDelete(key, version);
        this.registerCountedListener(0, delete, 0, false, null, futureCount);
        assertTrue("Delete worked", waitForCompletion(delete));
        // At this point, we have called all of the futures once
        assertEquals("Invoked all listeners", 0, futureCount.intValue());
        assertTrue("Delete worked a second time", waitForCompletion(delete));
        assertContainsVersioned("Get returned the same result", expected, waitForCompletion(get));
        assertEquals("Put returned the same result", version, waitForCompletion(put));
        assertEquals("Invoked all listeners only once", 0, futureCount.intValue());
    }
}
