package voldemort.store;

import voldemort.utils.ClosableIterator;
import voldemort.versioning.Versioned;

public interface StoreTransaction<T> {

    public ClosableIterator<T> getIterator() throws PersistenceFailureException;

    // public <T> void update(ClosableIterator<T> iter, Versioned<byte[]>
    // value);

    public void insert(ClosableIterator<T> iter, Versioned<byte[]> value)
            throws PersistenceFailureException;

    public void update(ClosableIterator<T> iter, Versioned<byte[]> value)
            throws PersistenceFailureException;

    public void close(boolean commit) throws PersistenceFailureException;
}
