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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class DistributingStore<N> implements Store<ByteArray, byte[]> {

    private static final Logger logger = LogManager.getLogger(DistributingStore.class);
    final Map<N, Store<ByteArray, byte[]>> stores;
    protected final ExecutorService executor;
    protected StoreDefinition storeDef;
    private final String name;
    protected final long timeout;
    protected final TimeUnit timeUnit;
    protected final int requiredReads;
    protected final int requiredWrites;
    protected final int preferredReads;
    protected final int preferredWrites;
    protected final Time time;

    public DistributingStore(String name,
                             Map<N, Store<ByteArray, byte[]>> stores,
                             StoreDefinition storeDef,
                             int numThreads,
                             long timeout,
                             TimeUnit units) {
        this(name,
             stores,
             storeDef,
             Executors.newFixedThreadPool(numThreads),
             timeout,
             units,
             SystemTime.INSTANCE);
    }

    public DistributingStore(String name,
                             Map<N, Store<ByteArray, byte[]>> stores,
                             StoreDefinition storeDef,
                             ExecutorService threadPool,
                             long timeout,
                             TimeUnit units,
                             Time time) {
        this(name,
             stores,
             threadPool,
             storeDef.getPreferredReads(),
             storeDef.getRequiredReads(),
             storeDef.getPreferredWrites(),
             storeDef.getRequiredWrites(),
             timeout,
             units,
             time);
    }

    public DistributingStore(String name,
                             Map<N, Store<ByteArray, byte[]>> stores,
                             ExecutorService threadPool,
                             int preferredReads,
                             int requiredReads,
                             int preferredWrites,
                             int requiredWrites,
                             long timeout,
                             TimeUnit units,
                             Time time) {
        this.time = time;
        this.name = name;
        this.executor = threadPool;
        this.requiredReads = requiredReads;
        this.preferredReads = preferredReads;
        this.requiredWrites = requiredWrites;
        this.preferredWrites = preferredWrites;
        this.timeout = timeout;
        this.timeUnit = units;
        this.stores = stores;
        validate("read", requiredReads, preferredReads);
        validate("write", requiredWrites, preferredWrites);
    }

    private void validate(final String operation, final int required, final int preferred) {
        if(required < 1) {
            throw new IllegalArgumentException("Cannot have a required " + operation
                                               + " less than 1.");
        } else if(preferred < required) {
            throw new IllegalArgumentException("Preferred " + operation
                                               + "  must be greater or equal to required "
                                               + operation + ".");
        } else if(preferred > stores.size()) {
            throw new IllegalArgumentException("Preferred " + operation
                                               + " is larger than the total number of nodes!");
        }
    }

    public String getName() {
        return this.name;
    }

    /**
     * Returns the store associated with the input node
     * 
     * @param node The node to retrieve the associated store
     * @return The store associated with the input node
     */
    protected Store<ByteArray, byte[]> getNodeStore(N node) {
        Store<ByteArray, byte[]> store = stores.get(node);
        return store;
    }

    public Map<N, Store<ByteArray, byte[]>> getNodeStores() {
        return stores;
    }

    public void putNodeStore(N node, Store<ByteArray, byte[]> store) {
        stores.put(node, store);
    }

    /**
     * Returns the list of values associated with the input key.
     * 
     * @param key The key to return the associated value for.
     * @return The list of values for the input key from the underlying stores.
     * @throws InsufficientSuccessfulNodesException if too few nodes respond
     */
    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        Set<N> nodes = stores.keySet();
        return get(key, nodes, preferredReads, requiredReads, timeout, timeUnit);
    }

    public List<Versioned<byte[]>> get(ByteArray key, Collection<N> nodes)
            throws VoldemortException {
        return get(key, nodes, preferredReads, requiredReads, timeout, timeUnit);
    }

    /**
     * Returns the list of values associated with the input key for the input
     * nodes
     * 
     * @param key The key to return the associated value for.
     * @param nodes The collection of nodes to retrieve the key from.
     * @param preferred The number of successful completions to wait for
     * @param required The number of successful completions required
     * @param timeout How long to wait for the tasks to complete
     * @return The list of values for the input key from the underlying stores.
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public List<Versioned<byte[]>> get(final ByteArray key,
                                       Collection<N> nodes,
                                       int preferred,
                                       int required,
                                       long timeout,
                                       TimeUnit units) throws VoldemortException {
        if(nodes.size() < required) {
            throw new InsufficientOperationalNodesException("Not enough nodes available",
                                                            nodes.size(),
                                                            required);
        }
        ParallelTask<N, List<Versioned<byte[]>>> job = getJob(key, nodes);
        Map<N, List<Versioned<byte[]>>> results = job.get(preferred, required, timeout, units);
        return buildResults(results);
    }

    protected List<Versioned<byte[]>> buildResults(Map<N, List<Versioned<byte[]>>> results) {
        List<Versioned<byte[]>> result = new ArrayList<Versioned<byte[]>>();
        for(List<Versioned<byte[]>> list: results.values()) {
            result.addAll(list);
        }
        return result;
    }

    /**
     * Returns the task responsible for retrieving the key from the input nodes
     * 
     * @param key The key to return the associated value for.
     * @param nodes The collection of nodes to retrieve the key from.
     * @return The distributed tasks for retrieving the values
     */
    protected ParallelTask<N, List<Versioned<byte[]>>> getJob(final ByteArray key,
                                                              Collection<N> nodes) {
        Map<N, Callable<List<Versioned<byte[]>>>> tasks = new HashMap<N, Callable<List<Versioned<byte[]>>>>();
        for(final N node: nodes) {
            final Store<ByteArray, byte[]> store = getNodeStore(node);
            if(store != null) {
                Callable<List<Versioned<byte[]>>> task = new Callable<List<Versioned<byte[]>>>() {

                    public List<Versioned<byte[]>> call() {
                        List<Versioned<byte[]>> result = store.get(key);
                        return result;
                    }
                };
                tasks.put(node, task);
            }
        }
        return ParallelTask.newInstance("GET", this.executor, tasks);
    }

    /**
     * Returns a map of key-values for the input keys for the input nodes
     * 
     * @param keys The keys to query values for.
     * @return The map of key-values for the input key from the underlying
     *         stores.
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        Set<N> nodes = stores.keySet();
        Map<N, List<ByteArray>> map = new HashMap<N, List<ByteArray>>(nodes.size());
        List<ByteArray> list = new ArrayList<ByteArray>();
        Iterator<ByteArray> iter = keys.iterator();
        while(iter.hasNext()) {
            list.add(iter.next());
        }
        for(N node: nodes) {
            map.put(node, list);
        }
        return getAll(map);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Map<N, List<ByteArray>> map)
            throws VoldemortException {
        return getAll(map, this.preferredReads, this.requiredReads, this.timeout, this.timeUnit);
    }

    /**
     * Returns the map of key-values associated with the input keys for the
     * input nodes
     * 
     * @param keys The keys to return the associated values for.
     * @param nodes The collection of nodes to retrieve the keys from.
     * @param preferred The number of successful completions to wait for
     * @param required The number of successful completions required
     * @param timeout How long to wait for the tasks to complete
     * @return The map of key-values for the input keys from the underlying
     *         stores.
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Map<N, List<ByteArray>> keys,
                                                          int preferred,
                                                          int required,
                                                          long timeout,
                                                          TimeUnit units) throws VoldemortException {
        ParallelTask<N, Map<ByteArray, List<Versioned<byte[]>>>> job = getAllJob(keys);
        Map<N, Map<ByteArray, List<Versioned<byte[]>>>> results = job.get(preferred,
                                                                          required,
                                                                          timeout,
                                                                          units);
        return buildResults(results);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> buildResults(Map<N, Map<ByteArray, List<Versioned<byte[]>>>> results) {
        Map<ByteArray, List<Versioned<byte[]>>> result = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        for(Map<ByteArray, List<Versioned<byte[]>>> values: results.values()) {
            for(ByteArray key: values.keySet()) {
                List<Versioned<byte[]>> current = result.get(key);
                if(current != null) {
                    current.addAll(values.get(key));
                } else {
                    result.put(key, values.get(key));
                }
            }
        }
        return result;

    }

    /**
     * Returns the task responsible for retrieving the key-values from the input
     * nodes
     * 
     * @param keys The keys to query values for.
     * @param nodes The collection of nodes to retrieve the keys from.
     * @return The distributed tasks for retrieving the values
     */
    public ParallelTask<N, Map<ByteArray, List<Versioned<byte[]>>>> getAllJob(Map<N, List<ByteArray>> map) {
        Map<N, Callable<Map<ByteArray, List<Versioned<byte[]>>>>> tasks = new HashMap<N, Callable<Map<ByteArray, List<Versioned<byte[]>>>>>(map.size());
        for(N node: map.keySet()) {
            final Store<ByteArray, byte[]> store = getNodeStore(node);
            if(store != null) {
                final Iterable<ByteArray> keys = map.get(node);
                Callable<Map<ByteArray, List<Versioned<byte[]>>>> task = new Callable<Map<ByteArray, List<Versioned<byte[]>>>>() {

                    public Map<ByteArray, List<Versioned<byte[]>>> call() {
                        return store.getAll(keys);
                    }
                };
                tasks.put(node, task);
            }
        }
        return ParallelTask.newInstance("GETALL", executor, tasks);
    }

    /**
     * Returns the list of versions associated with the input key.
     * 
     * @param key The key to return the associated versions for.
     * @return The list of versions for the input key from the underlying
     *         stores.
     * @throws InsufficientSuccessfulNodesException if too few nodes respond
     */
    public List<Version> getVersions(ByteArray key) throws VoldemortException {
        Set<N> nodes = stores.keySet();
        return getVersions(key, nodes, preferredReads, requiredReads, timeout, timeUnit);
    }

    /**
     * Returns the list of versions associated with the input key.
     * 
     * @param key The key to return the associated versions for.
     * @param nodes The collection of nodes to retrieve the versions from.
     * @return The list of versions for the input key from the underlying
     *         stores.
     * @throws InsufficientSuccessfulNodesException if too few nodes respond
     */
    public List<Version> getVersions(final ByteArray key, Collection<N> nodes)
            throws VoldemortException {
        return getVersions(key, nodes, preferredReads, requiredReads, timeout, timeUnit);
    }

    /**
     * Returns the map of key-values associated with the input keys for the
     * input nodes
     * 
     * @param keys The keys to return the associated values for.
     * @param nodes The collection of nodes to retrieve the keys from.
     * @param preferred The number of successful completions to wait for
     * @param required The number of successful completions required
     * @param timeout How long to wait for the tasks to complete
     * @return The map of key-values for the input keys from the underlying
     *         stores.
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public List<Version> getVersions(ByteArray key,
                                     Collection<N> nodes,
                                     int preferred,
                                     int required,
                                     long timeout,
                                     TimeUnit units) throws VoldemortException {
        ParallelTask<N, List<Version>> job = getVersionsJob(key, nodes);
        List<Version> result = new ArrayList<Version>();
        Map<N, List<Version>> results = job.get(preferred, required, timeout, units);
        for(List<Version> versions: results.values()) {
            result.addAll(versions);
        }
        return result;
    }

    /**
     * Returns the task responsible for retrieving the versions from the input
     * nodes
     * 
     * @param key The keys to query versions for.
     * @param nodes The collection of nodes to retrieve the versions from.
     * @return The distributed tasks for retrieving the versions
     */
    public ParallelTask<N, List<Version>> getVersionsJob(final ByteArray key, Collection<N> nodes) {
        Map<N, Callable<List<Version>>> tasks = new HashMap<N, Callable<List<Version>>>(nodes.size());
        for(N node: nodes) {
            final Store<ByteArray, byte[]> store = getNodeStore(node);
            if(store != null) {
                Callable<List<Version>> task = new Callable<List<Version>>() {

                    public List<Version> call() {
                        return store.getVersions(key);
                    }
                };
                tasks.put(node, task);
            }
        }
        return ParallelTask.newInstance("GETVERSIONS", executor, tasks);
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     * @return The version associated with the value in the store
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public Version put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        Set<N> nodes = stores.keySet();
        return put(key, value, nodes, preferredWrites, requiredWrites, timeout, timeUnit);
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     * @param nodes The collection of nodes to store the versions in.
     * @return The version associated with the value in the store
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public Version put(ByteArray key, Versioned<byte[]> value, Collection<N> nodes)
            throws VoldemortException {
        return put(key, value, nodes, preferredWrites, requiredWrites, timeout, timeUnit);
    }

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     * @param nodes The collection of nodes to store the versions in.
     * @param preferred The number of successful completions to wait for
     * @param required The number of successful completions required
     * @param timeout How long to wait for the tasks to complete
     * @return The version associated with the value in the store
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public Version put(ByteArray key,
                       Versioned<byte[]> value,
                       Collection<N> nodes,
                       int preferred,
                       int required,
                       long timeout,
                       TimeUnit units) throws VoldemortException {
        if(nodes.size() < required) {
            throw new InsufficientOperationalNodesException("Not enough nodes available",
                                                            nodes.size(),
                                                            required);
        }
        Version result = null;
        ParallelTask<N, Version> job = putJob(key, value, nodes);
        Map<N, Version> results = job.get(preferred, required, timeout, units);
        for(Version version: results.values()) {
            if(result != null) {
                // **TODO: What to do/check here??
            } else {
                result = version;
            }
        }
        return result;
    }

    /**
     * Returns the task responsible for associating the value with the key and
     * version in this store.
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     * @param nodes The collection of nodes to store the versions in.
     * @return The distributed tasks for associating keys with values.
     */
    public ParallelTask<N, Version> putJob(final ByteArray key,
                                           final Versioned<byte[]> value,
                                           Collection<N> nodes) {
        Map<N, Callable<Version>> tasks = new HashMap<N, Callable<Version>>();
        for(final N node: nodes) {
            final Store<ByteArray, byte[]> store = getNodeStore(node);
            if(store != null) {
                Callable<Version> task = new Callable<Version>() {

                    public Version call() {
                        Version result = store.put(key, value);
                        return result;
                    }
                };
                tasks.put(node, task);
            }
        }
        if(tasks.size() > 0) {
            return ParallelTask.newInstance("PUT", executor, tasks, false);
        } else {
            logger.error("Nothing for distributed store to do!");
            return null;
        }
    }

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current version of the key
     * @return True if anything was deleted, false otherwise
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        Set<N> nodes = stores.keySet();
        return delete(key, version, nodes, preferredWrites, requiredWrites, timeout, timeUnit);
    }

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current version of the key
     * @param nodes The collection of nodes to delete the entries from.
     * @return True if anything was deleted, false otherwise
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public boolean delete(ByteArray key, Version version, Collection<N> nodes)
            throws VoldemortException {
        return delete(key,
                      version,
                      nodes,
                      this.preferredWrites,
                      this.requiredWrites,
                      this.timeout,
                      this.timeUnit);
    }

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current version of the key
     * @param nodes The collection of nodes to delete the entries from.
     * @param preferred The number of successful completions to wait for
     * @param required The number of successful completions required
     * @param timeout How long to wait for the tasks to complete
     * @return True if anything was deleted, false otherwise
     * @throws InsufficientSuccessfulNodesException if too few nodes
     *         successfully respond
     */
    public boolean delete(ByteArray key,
                          Version version,
                          Collection<N> nodes,
                          int preferred,
                          int required,
                          long timeout,
                          TimeUnit units) throws VoldemortException {
        if(nodes.size() < required) {
            throw new InsufficientOperationalNodesException("Not enough nodes available",
                                                            nodes.size(),
                                                            required);
        }
        ParallelTask<N, Boolean> job = deleteJob(key, version, nodes);
        Map<N, Boolean> results = job.get(preferred, required, timeout, units);
        for(Boolean b: results.values()) {
            if(b.booleanValue()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the task responsible for removing the entries from the input
     * nodes
     * 
     * @param key The key to delete
     * @param version The current version of the key
     * @param nodes The collection of nodes to delete the entries from.
     * @return The distributed tasks for removing the entries
     */
    public ParallelTask<N, Boolean> deleteJob(final ByteArray key,
                                              final Version version,
                                              Collection<N> nodes) {
        Map<N, Callable<Boolean>> tasks = new HashMap<N, Callable<Boolean>>(nodes.size());
        for(N node: nodes) {
            final Store<ByteArray, byte[]> store = getNodeStore(node);
            if(store != null) {
                Callable<Boolean> task = new Callable<Boolean>() {

                    public Boolean call() {
                        return store.delete(key, version);
                    }
                };
                tasks.put(node, task);
            }
        }
        return ParallelTask.newInstance("DELETE", this.executor, tasks);

    }

    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case NODE_STORES:
                return this.stores;
            default:
                throw new NoSuchCapabilityException(capability, getName());
        }
    }

    public void close() {
        VoldemortException exception = null;
        for(Store<ByteArray, byte[]> client: stores.values()) {
            try {
                client.close();
            } catch(VoldemortException v) {
                exception = v;
            }
        }
        if(exception != null)
            throw exception;
    }

    /**
     * Checks that number of nodes containing the key is greater than the
     * required reads.
     * 
     * @param nodes The list of available nodes for some key.
     * 
     * @throws InsufficientOperationalNodesException In case of number of nodes
     *         is lesser than required reads.
     */
    protected void checkRequiredReads(final List<N> nodes)
            throws InsufficientOperationalNodesException {
        checkRequiredReads(nodes.size());
    }

    protected void checkRequiredReads(final int numNodes)
            throws InsufficientOperationalNodesException {
        if(numNodes < this.requiredReads) {
            if(logger.isDebugEnabled()) {
                logger.debug("quorom exception - not enough nodes required: " + requiredReads
                             + ", found: " + numNodes);
            }

            throw new InsufficientOperationalNodesException("Only "
                                                                    + numNodes
                                                                    + " nodes in preference list, but "
                                                                    + requiredReads
                                                                    + " reads required.",
                                                            numNodes,
                                                            requiredReads);
        }
    }

    /**
     * Checks that number of nodes containing the key is greater than the
     * required reads.
     * 
     * @param nodes The list of available nodes for some key.
     * 
     * @throws InsufficientOperationalNodesException In case of number of nodes
     *         is lesser than required reads.
     */
    protected void checkRequiredWrites(final List<N> nodes)
            throws InsufficientOperationalNodesException {
        checkRequiredWrites(nodes.size());
    }

    protected void checkRequiredWrites(final int numNodes)
            throws InsufficientOperationalNodesException {
        if(numNodes < this.requiredWrites) {
            if(logger.isDebugEnabled()) {
                logger.debug("quorom exception - not enough nodes required: " + requiredWrites
                             + ", found: " + numNodes);
            }

            throw new InsufficientOperationalNodesException("Only "
                                                                    + numNodes
                                                                    + " nodes in preference list, but "
                                                                    + requiredWrites
                                                                    + " writes required.",
                                                            numNodes,
                                                            requiredWrites);
        }
    }
}
