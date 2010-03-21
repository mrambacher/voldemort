package voldemort.store;

import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class StoreEntriesIterator extends StoreIterator<Pair<ByteArray, Versioned<byte[]>>> {

    public StoreEntriesIterator(StoreRow row) {
        super(row);
    }

    public Pair<ByteArray, Versioned<byte[]>> next() {
        this.advance();
        return new Pair<ByteArray, Versioned<byte[]>>(getKey(), getVersioned());
    }
}
