package voldemort.store.invalidmetadata;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

abstract public class MetadataCheckingStore<K, V, T> implements Store<K, V, T> {

    private final Logger logger = Logger.getLogger(MetadataCheckingStore.class);

    private final int metadataRefreshAttempts;
    private final String storeName;
    protected volatile Store<K, V, T> innerStore;

    public MetadataCheckingStore(String name, int retries) {
        this.storeName = name;
        this.metadataRefreshAttempts = retries;
    }

    public MetadataCheckingStore(String name, int retries, Store<K, V, T> inner) {
        this.storeName = name;
        this.metadataRefreshAttempts = retries;
        innerStore = inner;
    }

    public void bootStrap() {
        logger.info("bootstrapping metadata.");
        reinit();
    }

    abstract protected void reinit();

    public void close() {
        innerStore.close();
    }

    public String getName() {
        return storeName;
    }

    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.BOOT_STRAP) {
            return this;
        } else {
            return innerStore.getCapability(capability);
        }
    }

    public List<Versioned<V>> get(K key, T transforms) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return innerStore.get(key, transforms);
            } catch(InvalidMetadataException e) {
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return innerStore.getAll(keys, transforms);
            } catch(InvalidMetadataException e) {
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    public List<Version> getVersions(K key) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return innerStore.getVersions(key);
            } catch(InvalidMetadataException e) {
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    public Version put(K key, Versioned<V> versioned, T transforms) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return innerStore.put(key, versioned, transforms);
            } catch(InvalidMetadataException e) {
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }

    public boolean delete(K key, Version version) {
        for(int attempts = 0; attempts < this.metadataRefreshAttempts; attempts++) {
            try {
                return innerStore.delete(key, version);
            } catch(InvalidMetadataException e) {
                bootStrap();
            }
        }
        throw new VoldemortException(this.metadataRefreshAttempts
                                     + " metadata refresh attempts failed.");
    }
}
