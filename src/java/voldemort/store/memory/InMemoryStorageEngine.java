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

package voldemort.store.memory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableFilterIterator;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A simple non-persistent, in-memory store. Useful for unit testing.
 * 
 * 
 */
public class InMemoryStorageEngine<K, V, T> implements StorageEngine<K, V, T> {

    private final StoreDefinition storeDef;
    private final InMemoryStore<K, V, T> store;

    public InMemoryStorageEngine(StoreDefinition def) {
        storeDef = Utils.notNull(def);
        store = new InMemoryStore<K, V, T>(storeDef.getName());
    }

    public InMemoryStorageEngine(StoreDefinition def, ConcurrentMap<K, List<Versioned<V>>> map) {
        storeDef = Utils.notNull(def);
        store = new InMemoryStore<K, V, T>(storeDef.getName(), map);
    }

    public void close() {}

    public void deleteAll() {
        store.deleteAll();
    }

    public boolean delete(K key) {
        return delete(key, null);
    }

    public boolean delete(K key, Version version) {
        StoreUtils.assertValidKey(key);
        return store.delete(key, version);
    }

    public List<Version> getVersions(K key) {
        return store.getVersions(key);
    }

    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return store.get(key, transform);
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return store.getAll(keys, transforms);
    }

    public Version put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return store.put(key, value, transforms);
    }

    public Object getCapability(StoreCapabilityType capability) {
        return store.getCapability(capability);
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries(final VoldemortFilter filter) {
        ClosableIterator<Pair<K, Versioned<V>>> entries = new InMemoryIterator<K, V>(store.map);
        return new ClosableFilterIterator<Pair<K, Versioned<V>>>(entries) {

            @Override
            protected boolean matches(Pair<K, Versioned<V>> entry) {
                return filter.accept(entry.getFirst(), entry.getSecond());
            }
        };
    }

    public ClosableIterator<K> keys(final VoldemortFilter filter) {
        final Iterator<K> keys = store.map.keySet().iterator();
        ClosableIterator<K> iter = new ClosableFilterIterator<K>(keys) {

            @Override
            protected boolean matches(K key) {
                return filter.accept(key, null);
            }
        };
        return iter;
    }

    public void truncate() {
        store.deleteAll();
    }

    public String getName() {
        return store.getName();
    }

    @Override
    public String toString() {
        return store.toString();
    }

    @NotThreadsafe
    private static class InMemoryIterator<K, V> implements ClosableIterator<Pair<K, Versioned<V>>> {

        private final Iterator<Entry<K, List<Versioned<V>>>> iterator;
        private K currentKey;
        private Iterator<Versioned<V>> currentValues;

        public InMemoryIterator(ConcurrentMap<K, List<Versioned<V>>> map) {
            this.iterator = map.entrySet().iterator();
        }

        public boolean hasNext() {
            return hasNextInCurrentValues() || iterator.hasNext();
        }

        private boolean hasNextInCurrentValues() {
            return currentValues != null && currentValues.hasNext();
        }

        private Pair<K, Versioned<V>> nextInCurrentValues() {
            Versioned<V> item = currentValues.next();
            return Pair.create(currentKey, item);
        }

        public Pair<K, Versioned<V>> next() {
            if(hasNextInCurrentValues()) {
                return nextInCurrentValues();
            } else {
                // keep trying to get a next, until we find one (they could get
                // removed)
                while(true) {
                    Entry<K, List<Versioned<V>>> entry = iterator.next();

                    List<Versioned<V>> list = entry.getValue();
                    synchronized(list) {
                        // okay we may have gotten an empty list, if so try
                        // again
                        if(list.size() == 0)
                            continue;

                        // grab a snapshot of the list while we have exclusive
                        // access
                        currentValues = new ArrayList<Versioned<V>>(list).iterator();
                    }
                    currentKey = entry.getKey();
                    return nextInCurrentValues();
                }
            }
        }

        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        public void close() {
            // nothing to do here
        }

    }
}
