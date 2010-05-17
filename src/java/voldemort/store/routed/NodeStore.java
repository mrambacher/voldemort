/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright 2010 Nokia Corporation. All rights reserved.
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

package voldemort.store.routed;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Store which monitors requests and changes the state of the nodes.
 */
public class NodeStore<K, V> extends DelegatingStore<K, V> {

    private final Node node;
    private final FailureDetector failureDetector;

    /**
     * Creates a new node store
     * 
     * @param node The node being used
     * @param detector The failure detector for this node
     * @param inner The underlying store.
     */
    public NodeStore(Node node, FailureDetector detector, Store<K, V> inner) {
        super(inner);
        this.node = node;
        this.failureDetector = detector;
    }

    /**
     * Records an exception for a given node
     * 
     * @param startNs The time at which the operation started
     * @param e The caught exception
     */
    private void recordException(long startNs, UnreachableStoreException e) {
        failureDetector.recordException(node, (System.nanoTime() - startNs) / Time.NS_PER_MS, e);
    }

    /**
     * Records a successful operation for a given node
     * 
     * @param startNs The time at which the operation started
     */
    private void recordSuccess(long startNs) {
        failureDetector.recordSuccess(node, (System.nanoTime() - startNs) / Time.NS_PER_MS);
    }

    /**
     * Returns true if the node is currently available
     * 
     * @return True if the node is available, false otherwise.
     */
    public boolean isAvailable() {
        return this.failureDetector.isAvailable(node);
    }

    /**
     * Throws an exception if the node is currently not available.
     */
    private void checkNodeIsAvailable() {
        if(!isAvailable()) {
            throw new UnreachableStoreException("Node is down");
        }
    }

    /**
     * Saves the value for the key, updating the state of the node as
     * appropriate {@inheritDoc}
     */
    @Override
    public Version put(K key, Versioned<V> value) throws VoldemortException {
        checkNodeIsAvailable();
        long start = System.nanoTime();
        try {
            Version version = super.put(key, value);
            recordSuccess(start);
            return version;
        } catch(UnreachableStoreException e) {
            recordException(start, e);
            throw e;
        }
    }

    /**
     * Retrieves the versions for the key, updating the state of the node as
     * appropriate {@inheritDoc}
     */
    @Override
    public List<Version> getVersions(K key) throws VoldemortException {
        checkNodeIsAvailable();
        long start = System.nanoTime();
        try {
            List<Version> versions = super.getVersions(key);
            recordSuccess(start);
            return versions;
        } catch(UnreachableStoreException e) {
            recordException(start, e);
            throw e;
        }
    }

    /**
     * Retrieves the values for the key, updating the state of the node as
     * appropriate {@inheritDoc}
     */
    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        checkNodeIsAvailable();
        long start = System.nanoTime();
        try {
            List<Versioned<V>> value = super.get(key);
            recordSuccess(start);
            return value;
        } catch(UnreachableStoreException e) {
            recordException(start, e);
            throw e;
        }
    }

    /**
     * Retrieves the values for the keys, updating the state of the node as
     * appropriate {@inheritDoc}
     */
    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        checkNodeIsAvailable();
        long start = System.nanoTime();
        try {
            Map<K, List<Versioned<V>>> value = super.getAll(keys);
            recordSuccess(start);
            return value;
        } catch(UnreachableStoreException e) {
            recordException(start, e);
            throw e;
        }
    }

    /**
     * Deletes the version of the key, updating the state of the node as
     * appropriate {@inheritDoc}
     */
    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        checkNodeIsAvailable();
        long start = System.nanoTime();
        try {
            boolean deleted = super.delete(key, version);
            recordSuccess(start);
            return deleted;
        } catch(UnreachableStoreException e) {
            recordException(start, e);
            throw e;
        }
    }
}
