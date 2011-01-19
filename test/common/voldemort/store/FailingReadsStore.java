package voldemort.store;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.memory.InMemoryStore;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class FailingReadsStore<K, V, T> extends DelegatingStore<K, V, T> {

    private VoldemortException exception;

    public FailingReadsStore(String name) {
        this(new InMemoryStore<K, V, T>(name));
    }

    public FailingReadsStore(Store<K, V, T> inner) {
        this(inner, new VoldemortException("Operation failed"));
    }

    public FailingReadsStore(Store<K, V, T> inner, VoldemortException ex) {
        super(inner);
        this.exception = ex;
    }

    @Override
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        throw exception;
    }

    @Override
    public java.util.List<Version> getVersions(K key) {
        throw exception;
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        throw exception;
    }
}
