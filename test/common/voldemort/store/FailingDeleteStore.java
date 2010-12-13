package voldemort.store;

import voldemort.VoldemortException;
import voldemort.versioning.Version;

public class FailingDeleteStore<K, V, T> extends DelegatingStore<K, V, T> {

    public FailingDeleteStore(Store<K, V, T> inner) {
        super(inner);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }
}
