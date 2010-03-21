package voldemort.store;

import voldemort.utils.ByteArray;

public class StoreKeysIterator extends StoreIterator<ByteArray> {

    public StoreKeysIterator(StoreRow row) {
        super(row);
    }

    public ByteArray next() {
        this.advance();
        return getKey();
    }
}
