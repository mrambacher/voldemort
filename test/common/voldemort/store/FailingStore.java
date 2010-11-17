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
import java.util.Map;
import java.util.concurrent.Callable;

import voldemort.VoldemortException;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.CallableStore;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store that always throws an exception for every operation
 * 
 * 
 */
public class FailingStore<K, V> implements CallableStore<K, V> {

    private final String name;
    private final VoldemortException exception;

    public static <K, V> AsynchronousStore<K, V> asAsync(String name) {
        return asAsync(name, new VoldemortException("oops"));
    }

    public static <K, V> AsynchronousStore<K, V> asAsync(String name, VoldemortException e) {
        return AsyncUtils.asAsync(new FailingStore<K, V>(name, e));
    }

    public static <K, V> Store<K, V> asStore(String name) {
        return asStore(name, new VoldemortException("oops"));
    }

    public static <K, V> Store<K, V> asStore(String name, VoldemortException e) {
        return AsyncUtils.asStore(new FailingStore<K, V>(name, e));
    }

    public FailingStore(String name) {
        this(name, new VoldemortException("Operation failed!"));
    }

    public FailingStore(String name, VoldemortException e) {
        this.name = Utils.notNull(name);
        this.exception = e;
    }

    public void close() throws VoldemortException {
        throw exception;
    }

    public <R> R call(Callable<R> task) throws VoldemortException {
        throw exception;
    }

    public Callable<List<Versioned<V>>> callGet(K key) throws VoldemortException {
        return new Callable<List<Versioned<V>>>() {

            public List<Versioned<V>> call() {
                throw exception;
            }
        };
    }

    public String getName() {
        return name;
    }

    public Callable<Boolean> callDelete(K key, Version value) throws VoldemortException {
        return new Callable<Boolean>() {

            public Boolean call() {
                throw exception;
            }
        };
    }

    public Callable<Version> callPut(K key, Versioned<V> value) throws VoldemortException {
        return new Callable<Version>() {

            public Version call() {
                throw exception;
            }
        };
    }

    public Callable<Map<K, List<Versioned<V>>>> callGetAll(Iterable<K> keys)
            throws VoldemortException {
        return new Callable<Map<K, List<Versioned<V>>>>() {

            public Map<K, List<Versioned<V>>> call() {
                throw exception;
            }
        };
    }

    public Callable<List<Version>> callGetVersions(K key) {
        return new Callable<List<Version>>() {

            public List<Version> call() {
                throw exception;
            }
        };
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }
}
