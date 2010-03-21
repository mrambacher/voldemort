package voldemort.store;

import voldemort.versioning.Versioned;

public class StoreVersionedIterator extends StoreIterator<Versioned<byte[]>> {

    public StoreVersionedIterator(StoreRow row) {
        super(row);
    }

    public Versioned<byte[]> next() {
        this.advance();
        return getVersioned();
    }
}
