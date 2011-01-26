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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.store.AbstractStorageConfiguration;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.WrappedStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * A simple non-persistent, in-memory store. Useful for unit testing.
 * 
 * 
 */
public class InMemoryStorageEngine extends WrappedStorageEngine<ByteArray, byte[], byte[]> {

    private final AbstractStorageConfiguration config;

    public InMemoryStorageEngine(StoreDefinition def, AbstractStorageConfiguration config) {
        super(new InMemoryStore<ByteArray, byte[], byte[]>(def.getName()), def);
        this.config = config;
    }

    public InMemoryStorageEngine(StoreDefinition def,
                                 AbstractStorageConfiguration config,
                                 ConcurrentMap<ByteArray, List<Versioned<byte[]>>> map) {
        super(new InMemoryStore<ByteArray, byte[], byte[]>(def.getName(), map), def);
        this.config = config;
    }

    public void deleteAll() {
        InMemoryStore<ByteArray, byte[], byte[]> memory = (InMemoryStore<ByteArray, byte[], byte[]>) getStore();
        memory.deleteAll();
    }

    public boolean delete(ByteArray key) {
        return delete(key, null);
    }

    public <R> ClosableIterator<R> toIterator(final Iterator<R> iter) {
        return new ClosableIterator<R>() {

            public boolean hasNext() {
                return iter.hasNext();
            }

            public R next() {
                return iter.next();
            }

            public void remove() {
                iter.remove();
            }

            public void close() {
                // Do Nothing
            }
        };
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(final Collection<Integer> partitions,
                                                                        final VoldemortFilter<ByteArray, byte[]> filter,
                                                                        final byte[] transforms) {
        InMemoryStore<ByteArray, byte[], byte[]> memory = (InMemoryStore<ByteArray, byte[], byte[]>) getStore();
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries = new InMemoryIterator<ByteArray, byte[]>(memory.map);
        if(partitions != null && partitions.size() > 0) {
            entries = StoreUtils.entries(entries, config.getRoutingStrategy(getName()), partitions);
        }
        return StoreUtils.entries(entries, filter);
    }

    public ClosableIterator<ByteArray> keys(final Collection<Integer> partitions,
                                            final VoldemortFilter<ByteArray, byte[]> filter) {
        InMemoryStore<ByteArray, byte[], byte[]> memory = (InMemoryStore<ByteArray, byte[], byte[]>) getStore();
        ClosableIterator<ByteArray> keys = toIterator(memory.map.keySet().iterator());
        if(partitions != null && partitions.size() > 0) {
            keys = StoreUtils.keys(keys, config.getRoutingStrategy(getName()), partitions);
        }
        return StoreUtils.keys(keys, filter);
    }

    public void deleteEntries(VoldemortFilter<ByteArray, byte[]> filter) {
        StoreUtils.deleteEntries(this, filter);
    }

    public void deletePartitions(Collection<Integer> partitions) {
        StoreUtils.deletePartitions(this, partitions);
    }

    public void truncate() {
        deleteAll();
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
