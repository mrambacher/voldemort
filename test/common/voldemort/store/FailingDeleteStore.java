package voldemort.store;

import voldemort.VoldemortException;
import voldemort.versioning.Version;

public class FailingDeleteStore<K, V> extends DelegatingStore<K, V> {

    public FailingDeleteStore(Store<K, V> inner) {
        super(inner);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }
}
