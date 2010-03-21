package voldemort.store;

import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public interface StoreRow {

    public boolean advance();

    public boolean hasNext();

    public Versioned<byte[]> getVersioned();

    public Version getVersion();

    public ByteArray getKey();

    public void remove();

    public void close();
}
