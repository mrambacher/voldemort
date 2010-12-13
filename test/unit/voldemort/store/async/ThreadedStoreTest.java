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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import voldemort.VoldemortException;
import voldemort.store.FailingStore;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

import com.google.common.collect.Maps;

public class ThreadedStoreTest extends AbstractAsynchronousStoreTest {

    ExecutorService threadPool;

    public ThreadedStoreTest() {
        super("threaded");
        threadPool = Executors.newFixedThreadPool(10);

    }

    @Override
    public AsynchronousStore<ByteArray, byte[], byte[]> createAsyncStore(String name) {
        Maps.newHashMap();
        Store<ByteArray, byte[], byte[]> memory = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(name);
        CallableStore<ByteArray, byte[], byte[]> callable = new WrappedCallableStore<ByteArray, byte[], byte[]>(memory);
        AsynchronousStore<ByteArray, byte[], byte[]> threaded = new ThreadedStore<ByteArray, byte[], byte[]>(callable,
                                                                                                             threadPool);
        return threaded;
    }

    @Override
    protected AsynchronousStore<ByteArray, byte[], byte[]> createSlowStore(String name, long delay) {
        Store<ByteArray, byte[], byte[]> memory = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(name);
        Store<ByteArray, byte[], byte[]> sleepy = new SleepyStore<ByteArray, byte[], byte[]>(delay,
                                                                                             memory);

        AsynchronousStore<ByteArray, byte[], byte[]> threaded = new ThreadedStore<ByteArray, byte[], byte[]>(AsyncUtils.asCallable(sleepy),
                                                                                                             threadPool);
        return threaded;

    }

    @Override
    protected AsynchronousStore<ByteArray, byte[], byte[]> createFailingStore(String name,
                                                                              VoldemortException ex) {
        FailingStore<ByteArray, byte[], byte[]> failing = new FailingStore<ByteArray, byte[], byte[]>(name,
                                                                                                      ex);
        AsynchronousStore<ByteArray, byte[], byte[]> threaded = new ThreadedStore<ByteArray, byte[], byte[]>(failing,
                                                                                                             threadPool);
        return threaded;
    }
}
