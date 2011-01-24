package voldemort.store.pausable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A storage engine that can be paused to simulate a failure for testing. While
 * paused all operations on the store will block indefinitely. The methods to
 * pause and unpause are also exposed through JMX.
 * 
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 * @param <T> The type of the transforms
 */
public class PausableStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(PausableStorageEngine.class);

    private final InMemoryStorageEngine inner;
    private final Object condition = new Object();
    private volatile boolean paused;

    public PausableStorageEngine(InMemoryStorageEngine inner) {
        super();
        this.inner = inner;
    }

    public StoreDefinition getStoreDefinition() {
        return inner.getStoreDefinition();
    }

    public void close() throws VoldemortException {
        inner.close();
    }

    public boolean delete(ByteArray key, Version version) {
        blockIfNecessary();
        return inner.delete(key);
    }

    private void blockIfNecessary() {
        synchronized(condition) {
            while(paused) {
                try {
                    condition.wait();
                } catch(InterruptedException e) {
                    throw new VoldemortException("Pausable store interrupted while paused.");
                }
            }
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) {
        blockIfNecessary();
        return inner.get(key, transforms);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms) {
        blockIfNecessary();
        return inner.getAll(keys, transforms);
    }

    public Version put(ByteArray key, Versioned<byte[]> value, byte[] transforms) {
        blockIfNecessary();
        return inner.put(key, value, transforms);
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(final Collection<Integer> partitions,
                                                                        final VoldemortFilter filter,
                                                                        final byte[] transforms) {
        blockIfNecessary();
        return inner.entries(partitions, filter, transforms);
    }

    public ClosableIterator<ByteArray> keys(final Collection<Integer> partitions,
                                            final VoldemortFilter filter) {
        blockIfNecessary();
        return inner.keys(partitions, filter);
    }

    public void truncate() {
        blockIfNecessary();
        inner.deleteAll();
    }

    public List<Version> getVersions(ByteArray key) {
        blockIfNecessary();
        return inner.getVersions(key);
    }

    public Object getCapability(StoreCapabilityType capability) {
        return inner.getCapability(capability);
    }

    public String getName() {
        return inner.getName();
    }

    @JmxOperation(description = "Pause all operations on the storage engine.")
    public void pause() {
        logger.info("Pausing store '" + getName() + "'.");
        paused = true;
    }

    @JmxOperation(description = "Unpause the storage engine.")
    public void unpause() {
        logger.info("Unpausing store '" + getName() + "'.");
        paused = false;
        synchronized(condition) {
            condition.notifyAll();
        }
    }

}
