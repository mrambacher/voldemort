package voldemort.store;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class RandomlyFailingDelegatingStore<K, V, T> extends DelegatingStore<K, V, T> implements
        StorageEngine<K, V, T> {

    private static double FAIL_PROBABILITY = 0.60;
    private final StorageEngine<K, V, T> innerStorageEngine;

    public RandomlyFailingDelegatingStore(StorageEngine<K, V, T> innerStorageEngine) {
        super(innerStorageEngine);
        this.innerStorageEngine = innerStorageEngine;
    }

    public StoreDefinition getStoreDefinition() {
        return innerStorageEngine.getStoreDefinition();
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries(final VoldemortFilter filter) {
        return new ClosableIterator<Pair<K, Versioned<V>>>() {

            ClosableIterator<Pair<K, Versioned<V>>> iterator = innerStorageEngine.entries(filter);

            public void close() {
                iterator.close();
            }

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public Pair<K, Versioned<V>> next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            public void remove() {}
        };
    }

    public ClosableIterator<K> keys(final VoldemortFilter filter) {
        return new ClosableIterator<K>() {

            ClosableIterator<K> iterator = innerStorageEngine.keys(filter);

            public void close() {
                iterator.close();
            }

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public K next() {
                if(Math.random() > FAIL_PROBABILITY)
                    return iterator.next();

                throw new VoldemortException("Failing now !!");
            }

            public void remove() {}
        };
    }

    public void truncate() {
        if(Math.random() > FAIL_PROBABILITY) {
            innerStorageEngine.truncate();
        }

        throw new VoldemortException("Failing now !!");
    }
}