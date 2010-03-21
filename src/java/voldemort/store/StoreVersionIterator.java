package voldemort.store;

import voldemort.versioning.Version;

public class StoreVersionIterator extends StoreIterator<Version> {

    public StoreVersionIterator(StoreRow row) {
        super(row);
    }

    public Version next() {
        this.advance();
        return getVersion();
    }
}
