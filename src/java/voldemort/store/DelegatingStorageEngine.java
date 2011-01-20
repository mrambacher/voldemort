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

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class DelegatingStorageEngine<K, V, T> implements StorageEngine<K, V, T> {

    private final StorageEngine<K, V, T> inner;

    protected DelegatingStorageEngine(StorageEngine<K, V, T> inner) {
        this.inner = inner;
    }

    public StoreDefinition getStoreDefinition() {
        return inner.getStoreDefinition();
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries(VoldemortFilter filter) {
        return inner.entries(filter);
    }

    public ClosableIterator<K> keys(VoldemortFilter filter) {
        return inner.keys(filter);
    }

    public void truncate() {
        inner.truncate();
    }

    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        return inner.get(key, transforms);
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        return inner.getAll(keys, transforms);
    }

    public Version put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        return inner.put(key, value, transforms);
    }

    public boolean delete(K key, Version version) throws VoldemortException {
        return inner.delete(key, version);
    }

    public String getName() {
        return inner.getName();
    }

    public void close() throws VoldemortException {
        inner.close();
    }

    public Object getCapability(StoreCapabilityType capability) {
        return inner.getCapability(capability);
    }

    public List<Version> getVersions(K key) {
        return inner.getVersions(key);
    }
}
