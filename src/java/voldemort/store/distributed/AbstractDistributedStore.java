package voldemort.store.distributed;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

abstract public class AbstractDistributedStore<N, K, V, T> implements DistributedStore<N, K, V, T> {

    protected final boolean makeUnique;
    /** The map of nodes to asynchronous stores */
    final Map<N, AsynchronousStore<K, V, T>> nodeStores;
    protected final ResultsBuilder<N, List<Versioned<V>>> getBuilder;
    protected final ResultsBuilder<N, List<Version>> versionsBuilder;
    protected final ResultsBuilder<N, Version> putBuilder;
    protected final ResultsBuilder<N, Boolean> deleteBuilder;
    protected final Logger logger = LogManager.getLogger(getClass());

    private final StoreDefinition storeDef;

    public AbstractDistributedStore(Map<N, AsynchronousStore<K, V, T>> stores,
                                    StoreDefinition storeDef,
                                    boolean unique) {
        this.storeDef = storeDef;
        this.nodeStores = stores;
        this.makeUnique = unique;
        this.getBuilder = DistributedStoreFactory.GetBuilder(unique);
        this.versionsBuilder = DistributedStoreFactory.GetVersionsBuilder(unique);
        this.putBuilder = DistributedStoreFactory.PutBuilder();
        this.deleteBuilder = DistributedStoreFactory.DeleteBuilder();
    }

    /**
     * Returns the nodes for distribution for the input key
     * 
     * @param key The key for the request
     * @return All of the nodes.
     */
    public Collection<N> getNodesForKey(final K key) {
        Map<N, AsynchronousStore<K, V, T>> stores = getNodeStores();
        return stores.keySet();
    }

    public AsynchronousStore<K, V, T> getNodeStore(N node) {
        return nodeStores.get(node);
    }

    public Map<N, AsynchronousStore<K, V, T>> getNodeStores() {
        return nodeStores;
    }

    protected StoreFuture<List<Versioned<V>>> submitGet(N node, final K key, final T transform) {
        AsynchronousStore<K, V, T> async = nodeStores.get(node);
        return async.submitGet(key, transform);
    }

    abstract protected DistributedFuture<N, List<Versioned<V>>> distributeGet(Collection<N> nodes,
                                                                              K key,
                                                                              T transform,
                                                                              int preferred,
                                                                              int required);

    public DistributedFuture<N, List<Versioned<V>>> submitGet(final K key,
                                                              final T transform,
                                                              Collection<N> nodes,
                                                              int preferred,
                                                              int required)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredReads(nodes, required);
        return distributeGet(nodes, key, transform, preferred, required);
    }

    protected StoreFuture<Map<K, List<Versioned<V>>>> submitGetAll(N node,
                                                                   final List<K> keys,
                                                                   final Map<K, T> transforms) {
        AsynchronousStore<K, V, T> async = nodeStores.get(node);
        return async.submitGetAll(keys, transforms);
    }

    abstract protected DistributedFuture<N, Map<K, List<Versioned<V>>>> distributeGetAll(Map<N, List<K>> nodesToKeys,
                                                                                         Map<K, T> transforms,
                                                                                         int preferred,
                                                                                         int required);

    public DistributedFuture<N, Map<K, List<Versioned<V>>>> submitGetAll(final Map<N, List<K>> nodesToKeys,
                                                                         final Map<K, T> transforms,
                                                                         int preferred,
                                                                         int required)
            throws VoldemortException {
        if(nodesToKeys == null) {
            throw new IllegalArgumentException("Nodes cannot be null.");
        }
        DistributedStoreFactory.assertValidNodes(nodesToKeys.keySet(),
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        return distributeGetAll(nodesToKeys, transforms, preferred, required);
    }

    protected StoreFuture<Version> submitPut(N node,
                                             final K key,
                                             final Versioned<V> value,
                                             final T transform) {
        AsynchronousStore<K, V, T> async = nodeStores.get(node);
        return async.submitPut(key, value, transform);
    }

    abstract protected DistributedFuture<N, Version> distributePut(Collection<N> nodes,
                                                                   K key,
                                                                   Versioned<V> value,
                                                                   T transform,
                                                                   int preferred,
                                                                   int required);

    public DistributedFuture<N, Version> submitPut(K key,
                                                   Versioned<V> value,
                                                   T transform,
                                                   Collection<N> nodes,
                                                   int preferred,
                                                   int required) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredWrites(nodes, required);
        return distributePut(nodes, key, value, transform, preferred, required);
    }

    protected void checkRequiredReads(final Collection<N> nodes, int required)
            throws InsufficientOperationalNodesException {
        DistributedStoreFactory.checkRequiredReads(nodes.size(), required);
    }

    protected void checkRequiredWrites(final Collection<N> nodes, int required)
            throws InsufficientOperationalNodesException {
        DistributedStoreFactory.checkRequiredWrites(nodes.size(), required);
    }

    protected StoreFuture<Boolean> submitDelete(N node, final K key, final Version version) {
        AsynchronousStore<K, V, T> async = nodeStores.get(node);
        return async.submitDelete(key, version);
    }

    abstract protected DistributedFuture<N, Boolean> distributeDelete(Collection<N> nodes,
                                                                      K key,
                                                                      Version version,
                                                                      int preferred,
                                                                      int required);

    public DistributedFuture<N, Boolean> submitDelete(K key,
                                                      Version version,
                                                      Collection<N> nodes,
                                                      int preferred,
                                                      int required) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredWrites(nodes, required);
        return distributeDelete(nodes, key, version, preferred, required);
    }

    protected StoreFuture<List<Version>> submitGetVersions(N node, final K key) {
        AsynchronousStore<K, V, T> async = nodeStores.get(node);
        return async.submitGetVersions(key);
    }

    abstract protected DistributedFuture<N, List<Version>> distributeGetVersions(Collection<N> nodes,
                                                                                 K key,
                                                                                 int preferred,
                                                                                 int required);

    public DistributedFuture<N, List<Version>> submitGetVersions(K key,
                                                                 Collection<N> nodes,
                                                                 int preferred,
                                                                 int required)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DistributedStoreFactory.assertValidNodes(nodes,
                                                 this.nodeStores.keySet(),
                                                 preferred,
                                                 required);
        checkRequiredReads(nodes, required);
        return distributeGetVersions(nodes, key, preferred, required);
    }

    /**
     * @return The name of the store.
     */
    public String getName() {
        return storeDef.getName();
    }

    /**
     * Close the store.
     * 
     * @throws VoldemortException If closing fails.
     */
    public void close() throws VoldemortException {
        VoldemortException exception = null;
        for(AsynchronousStore<K, V, T> async: nodeStores.values()) {
            try {
                async.close();
            } catch(VoldemortException e) {
                exception = e;
            }
        }
        if(exception != null) {
            throw exception;
        }
    }

    /**
     * Get some capability of the store. Examples would be the serializer used,
     * or the routing strategy. This provides a mechanism to verify that the
     * store hierarchy has some set of capabilities without knowing the precise
     * layering.
     * 
     * @param capability The capability type to retrieve
     * @return The given capaiblity
     * @throws NoSuchCapabilityException if the capaibility is not present
     */
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case NODE_STORES:
                return this.nodeStores;
            case SYNCHRONOUS_NODE_STORES:
                return AsyncUtils.asStores(nodeStores);
            case ASYNCHRONOUS:
                return this;
            default:
                throw new NoSuchCapabilityException(capability, getName());
        }
    }
}
