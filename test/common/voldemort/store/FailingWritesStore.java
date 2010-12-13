package voldemort.store;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class FailingWritesStore<K, V, T> extends DelegatingStore<K, V, T> {

    private VoldemortException exception;

    public FailingWritesStore(Store<K, V, T> inner) {
        this(inner, new VoldemortException("Operation failed"));
    }

    public FailingWritesStore(Store<K, V, T> inner, VoldemortException ex) {
        super(inner);
        this.exception = ex;
    }

    @Override
    public Version put(K key, Versioned<V> data, T transform) throws VoldemortException {
        throw this.exception;
    }
}