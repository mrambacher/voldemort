package voldemort.client;

import voldemort.store.invalidmetadata.MetadataCheckingStore;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

public class MetadataRefreshingStore<K, V> extends MetadataCheckingStore<K, V> {

    private final StoreClientFactory storeFactory;
    private final InconsistencyResolver<Versioned<V>> resolver;

    public MetadataRefreshingStore(String name,
                                   InconsistencyResolver<Versioned<V>> resolver,
                                   StoreClientFactory factory,
                                   int retries) {
        super(name, retries);
        this.storeFactory = factory;
        this.resolver = resolver;
        reinit();
    }

    @Override
    protected void reinit() {
        innerStore = storeFactory.getRawStore(getName(), resolver);

    }
}
