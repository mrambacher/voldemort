package voldemort.store;

import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

abstract public class StoreIterator<T> implements ClosableIterator<T> {

    StoreRow row;

    public StoreIterator(StoreRow row) {
        this.row = row;
    }

    @Override
    protected void finalize() {
        close();
    }

    public ByteArray getKey() throws PersistenceFailureException {
        return row.getKey();
    }

    public Version getVersion() throws PersistenceFailureException {
        return row.getVersion();
    }

    public Versioned<byte[]> getVersioned() throws PersistenceFailureException {
        return row.getVersioned();
    }

    public boolean hasNext() {
        return row.hasNext();
    }

    protected void advance() {
        if(!row.advance()) {
            throw new PersistenceFailureException("Advanced past the end of the iterator");
        }
    }

    public void remove() {
        row.remove();
    }

    public void close() {
        if(row != null) {
            row.close();
            row = null;
        }
    }

}
