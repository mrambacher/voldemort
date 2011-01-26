package voldemort.store;

import java.util.Collection;

import org.apache.log4j.Logger;

import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

abstract public class AbstractStorageConfiguration implements StorageConfiguration {

    protected final Logger logger = Logger.getLogger(getClass());

    protected final VoldemortConfig voldemortConfig;

    public AbstractStorageConfiguration(VoldemortConfig config) {
        this.voldemortConfig = config;
    }

    public VoldemortConfig getConfig() {
        return voldemortConfig;
    }

    public RoutingStrategy getRoutingStrategy(String storeName) {
        MetadataStore metadata = voldemortConfig.getMetadata();
        return metadata.getRoutingStrategy(storeName);
    }

    public ClosableIterator<ByteArray> keys(String storeName,
                                            ClosableIterator<ByteArray> iter,
                                            final Collection<Integer> partitions) {
        if(partitions != null && partitions.size() > 0) {
            RoutingStrategy strategy = getRoutingStrategy(storeName);
            return StoreUtils.keys(iter, strategy, partitions);
        } else {
            return iter;
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(String storeName,
                                                                        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter,
                                                                        final Collection<Integer> partitions) {
        if(partitions != null && partitions.size() > 0) {
            RoutingStrategy strategy = getRoutingStrategy(storeName);
            return StoreUtils.entries(iter, strategy, partitions);
        } else {
            return iter;
        }
    }
}
