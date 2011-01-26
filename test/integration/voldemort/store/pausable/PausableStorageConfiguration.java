package voldemort.store.pausable;

import voldemort.server.VoldemortConfig;
import voldemort.store.AbstractStorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

/**
 * The storage configuration for the PausableStorageEngine
 * 
 * 
 */
public class PausableStorageConfiguration extends AbstractStorageConfiguration {

    private static final String TYPE_NAME = "pausable";

    public PausableStorageConfiguration(VoldemortConfig config) {
        super(config);
    }

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef) {

        return new PausableStorageEngine(new InMemoryStorageEngine(storeDef, this));
    }

    public String getType() {
        return TYPE_NAME;
    }

}
