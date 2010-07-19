package voldemort.store.versioned;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Version;

public class VectorClockResolvingStore<K, V> extends InconsistencyResolvingStore<K, V> {

    public VectorClockResolvingStore(Store<K, V> innerStore) {
        super(innerStore, new VectorClockInconsistencyResolver<V>());
    }

    @Override
    public List<Version> getVersions(K key) throws VoldemortException {
        List<Version> items = super.getVersions(key);
        return VectorClockInconsistencyResolver.getVersions(items);
    }
}
