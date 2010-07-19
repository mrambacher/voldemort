package voldemort.store.routed;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class NodeValueTest extends TestCase {

    private final static Logger logger = Logger.getLogger(NodeValueTest.class);

    public static Version createVersion(int nodeId, int clockEntry) {
        VectorClock version = TestUtils.getClock(nodeId);
        for(int i = 0; i < clockEntry - 1; i++) {
            TestUtils.increment(version, nodeId);
        }
        return version;
    }

    public static NodeValue<Integer, ByteArray, byte[]> createNodeValue(int nodeId, Version version) {
        ByteArray key = new ByteArray(new byte[] { 54, 52, 49, 57, 52, 54, 51, 51 });
        byte[] value = new byte[] { 69, 119, 105, 119, 83, 71, 80, 113, 108, 108, 119, 86, 67, 120,
                103, 111 };
        Versioned<byte[]> versioned = new Versioned<byte[]>(value, version);
        return new NodeValue<Integer, ByteArray, byte[]>(nodeId, key, versioned);
    }

    @Test
    public void testHashCodeEquals() {
        // Create multiple nodeValues that are equal and see if they hash the
        // same and equal each other
        Version v1 = createVersion(1, 6);
        Version v2 = createVersion(1, 6);
        Version v3 = createVersion(1, 7);

        logger.info("v1 = " + v1);
        logger.info("v2 = " + v2);
        logger.info("v3 = " + v3);

        assertEquals("VectorClock#equals works", v1, v2);
        assertEquals("VectorClock#hashCode works", v1.hashCode(), v2.hashCode());

        assertFalse(v1.equals(v3));
        assertFalse(v1.hashCode() == v3.hashCode());

        NodeValue<Integer, ByteArray, byte[]> nv1 = createNodeValue(1, v1);
        NodeValue<Integer, ByteArray, byte[]> nv2 = createNodeValue(1, v2);
        NodeValue<Integer, ByteArray, byte[]> nv3 = createNodeValue(1, v3);

        logger.info("nv1 = " + nv1);
        logger.info("nv2 = " + nv2);
        logger.info("nv3 = " + nv3);

        assertEquals("NodeValue#equals works", nv1, nv2);
        assertEquals("NodeValue#hashCode works", nv1.hashCode(), nv2.hashCode());

        assertFalse(nv1.equals(nv3));
        assertFalse(nv1.hashCode() == nv3.hashCode());
    }

    @Test
    public void testMultimap() {
        Multimap<Version, NodeValue<Integer, ByteArray, byte[]>> multimap = HashMultimap.create();
        Version version = createVersion(1, 6);
        NodeValue<Integer, ByteArray, byte[]> nodeValue = createNodeValue(1, version);
        multimap.put(version, nodeValue);

        Version version2 = createVersion(1, 7);
        NodeValue<Integer, ByteArray, byte[]> nodeValue2 = createNodeValue(1, version2);

        multimap.put(version2, nodeValue2);
        multimap.removeAll(version2);

        logger.info(multimap);

        assertTrue("Multimap#containsKey() works", multimap.containsKey(version));
        assertTrue("Multimap#containsEntry() works", multimap.containsEntry(version, nodeValue));

        for(Version v: multimap.keySet()) {
            logger.info(v);
            assertTrue("Multimap#get(v) returns a non-empty iterator", multimap.get(v)
                                                                               .iterator()
                                                                               .hasNext());

            NodeValue<Integer, ByteArray, byte[]> nv = multimap.get(v).iterator().next();
            logger.info(nv);
        }

    }

    @Test
    public void testClone() throws Exception {
        NodeValue<String, Integer> value = new NodeValue<String, Integer>(1,
                                                                          "hello",
                                                                          new Versioned<Integer>(1));
        NodeValue<String, Integer> clone = value.clone();

        assertEquals("Clones match", value, clone);
        assertEquals("Hashes match", value.hashCode(), clone.hashCode());
    }

    @Test
    public void testNodeInequality() throws Exception {
        NodeValue<String, Integer> value1 = new NodeValue<String, Integer>(1,
                                                                           "hello",
                                                                           new Versioned<Integer>(1));
        NodeValue<String, Integer> value2 = new NodeValue<String, Integer>(2,
                                                                           "hello",
                                                                           new Versioned<Integer>(1));

        assertNotSame("Values match", value1, value2);
        assertNotSame("Hashes match", value1.hashCode(), value2.hashCode());
    }

    @Test
    public void testVersionInequality() throws Exception {
        NodeValue<String, Integer> value1 = new NodeValue<String, Integer>(1,
                                                                           "hello",
                                                                           new Versioned<Integer>(1,
                                                                                                  TestUtils.getClock(1)));
        NodeValue<String, Integer> value2 = new NodeValue<String, Integer>(1,
                                                                           "hello",
                                                                           new Versioned<Integer>(1,
                                                                                                  TestUtils.getClock(2)));

        assertNotSame("Values match", value1, value2);
        assertNotSame("Hashes match", value1.hashCode(), value2.hashCode());
    }

    @Test
    public void testKeyInequality() throws Exception {
        NodeValue<String, Integer> value1 = new NodeValue<String, Integer>(1,
                                                                           "hello",
                                                                           new Versioned<Integer>(1,
                                                                                                  TestUtils.getClock(1)));
        NodeValue<String, Integer> value2 = new NodeValue<String, Integer>(1,
                                                                           "so long",
                                                                           new Versioned<Integer>(1,
                                                                                                  TestUtils.getClock(1)));

        assertNotSame("Values match", value1, value2);
        assertNotSame("Hashes match", value1.hashCode(), value2.hashCode());
    }
}
