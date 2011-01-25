package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

abstract public class WrappedStorageEngine<K, V, T> implements StorageEngine<K, V, T> {

    protected final Store<K, V, T> store;
    protected final StoreDefinition storeDef;

    protected WrappedStorageEngine(Store<K, V, T> store, StoreDefinition storeDef) {
        this.store = store;
        this.storeDef = storeDef;
    }

    protected Store<K, V, T> getStore() {
        return store;
    }

    public StoreDefinition getStoreDefinition() {
        return storeDef;
    }

    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        return store.get(key, transforms);
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        return store.getAll(keys, transforms);
    }

    public Version put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        return store.put(key, value, transforms);
    }

    public boolean delete(K key, Version version) throws VoldemortException {
        return store.delete(key, version);
    }

    public String getName() {
        return store.getName();
    }

    public void close() throws VoldemortException {
        store.close();
    }

    public Object getCapability(StoreCapabilityType capability) {
        return store.getCapability(capability);
    }

    public List<Version> getVersions(K key) {
        return store.getVersions(key);
    }
}
