package voldemort.store;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class FailingWritesStore<K, V> extends DelegatingStore<K, V> {

    private VoldemortException exception;

    public FailingWritesStore(Store<K, V> inner) {
        this(inner, new VoldemortException("Operation failed"));
    }

    public FailingWritesStore(Store<K, V> inner, VoldemortException ex) {
        super(inner);
        this.exception = ex;
    }

    @Override
    public Version put(K key, Versioned<V> data) throws VoldemortException {
        throw this.exception;
    }
}