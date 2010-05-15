package voldemort.versioning;

import voldemort.TestUtils;
import voldemort.serialization.Serializer;

public class MetadataTest extends VersionedTest implements Serializer<Integer> {

    public byte[] toBytes(Integer i) {
        String s = i.toString();
        return s.getBytes();
    }

    public Integer toObject(byte[] bytes) {
        String s = new String(bytes);
        return Integer.parseInt(s);
    }

    @Override
    protected Versioned<Integer> getVersioned(Integer value, int... increments) {
        Versioned<Integer> versioned = new Versioned<Integer>(value, TestUtils.getClock(increments));
        for(int i: increments) {
            versioned.getMetadata().setProperty(Integer.toString(i), Integer.toString(i));
        }
        return versioned;
    }

    public void testMetadata() {
        Versioned<Integer> first = getVersioned(1, 1);
        first.getMetadata().setProperty("hello", "world");
        assertEquals(first.getMetadata().getProperty("hello"), "world");
        assertNotSame("Null versioneds not equal.", first, getVersioned(1, 1));
        first.getMetadata().removeProperty("hello");
        assertNull("Removed property does not exist", first.getMetadata().getProperty("hello"));
    }

    public void testMetadataSerialization() {
        Versioned<Integer> first = getVersioned(1, 1);
        Metadata metadata = first.getMetadata();
        metadata.setProperty("1", "1");
        metadata.setProperty("2", "2");
        assertEquals(metadata, VersionFactory.toMetadata(metadata.toBytes()));
    }

    public void testSerialization() {
        Versioned<Integer> foo = getVersioned(1, 1);
        Versioned<Integer> bar = VersionFactory.toVersioned(VersionFactory.toBytes(foo, this), this);

        assertEquals("Serialized metadata are equal", foo, bar);
        Versioned<Integer> first = getVersioned(1, 1);
        first.getMetadata().setProperty("hello", "world");
        Versioned<Integer> second = VersionFactory.toVersioned(VersionFactory.toBytes(first, this),
                                                               this);
        assertEquals("Serialized metadata are equal", first, second);
        assertNotNull("Properties came back", second.getMetadata().getProperty("hello"));
    }
}
