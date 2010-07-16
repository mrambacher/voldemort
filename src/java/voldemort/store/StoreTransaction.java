package voldemort.store;

import voldemort.versioning.Versioned;

public interface StoreTransaction<T> {

    public StoreIterator<T> getIterator() throws PersistenceFailureException;

    public void insert(StoreIterator<T> iter, Versioned<byte[]> value)
            throws PersistenceFailureException;

    public void update(StoreIterator<T> iter, Versioned<byte[]> value)
            throws PersistenceFailureException;

    public void close(boolean commit) throws PersistenceFailureException;
}
