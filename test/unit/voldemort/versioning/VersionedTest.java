/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.versioning;

import static voldemort.TestUtils.getClock;
import junit.framework.TestCase;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.serialization.StringSerializer;

public class VersionedTest extends TestCase {

    protected Versioned<Integer> getVersioned(Integer value, int... versionIncrements) {
        return new Versioned<Integer>(value, TestUtils.getClock(versionIncrements));
    }

    public void mustHaveVersion() {
        try {
            new Versioned<Integer>(1, null);
            fail("Successfully created Versioned with null version.");
        } catch(NullPointerException e) {
            // this is good
        }
    }

    @Test
    public void testEquals() {
        assertEquals("Null versioneds not equal.", getVersioned(null), getVersioned(null));
        assertEquals("equal versioneds not equal.", getVersioned(1), getVersioned(1));
        assertEquals("equal versioneds not equal.", getVersioned(1, 1, 2), getVersioned(1, 1, 2));

        assertTrue("Equals values with different version are equal!",
                   !getVersioned(1, 1, 2).equals(getVersioned(1, 1, 2, 2)));
        assertTrue("Different values with same version are equal!",
                   !getVersioned(1, 1, 2).equals(getVersioned(2, 1, 2)));
        assertTrue("Different values with different version are equal!",
                   !getVersioned(1, 1, 2).equals(getVersioned(2, 1, 1, 2)));

        // Should work for array types too!
        assertEquals("Equal arrays are not equal!",
                     new Versioned<byte[]>(new byte[] { 1 }),
                     new Versioned<byte[]>(new byte[] { 1 }));
    }

    @Test
    public void testClone() {
        Versioned<Integer> v1 = getVersioned(2, 1, 2, 3);
        Versioned<Integer> v2 = v1.cloneVersioned();
        assertEquals(v1, v2);
        assertTrue(v1 != v2);
        assertTrue(v1.getVersion() != v2.getVersion());
        ((VectorClock) v2.getVersion()).incrementClock(1, System.currentTimeMillis());
        assertTrue(!v1.equals(v2));
    }

    public static void assertEquals(String message,
                                    Versioned<byte[]> expected,
                                    Versioned<byte[]> result) {
        assertEquals(message, expected.getVersion(), result.getVersion());
        assertTrue(message, TestUtils.bytesEqual(expected.getValue(), result.getValue()));
    }

    @Test
    public void testByteSerialization() {
        Version version = getClock(2, 1, 2, 3);
        Versioned<byte[]> versioned = new Versioned<byte[]>("abcde".getBytes(), version);
        byte[] serialized = VectorClockVersionSerializer.toBytes(versioned);
        assertEquals("The versioned serializes to itself using original protocol.",
                     versioned,
                     VersionFactory.toVersioned(serialized));
        serialized = VectorClockProtoSerializer.toBytes(versioned);
        assertEquals("The versioned serializes to itself using protobuf protocol.",
                     versioned,
                     VersionFactory.toVersioned(serialized));
    }

    @Test
    public void testStringSerialization() {
        Version version = getClock(2, 1, 2, 3);
        Versioned<String> versioned = new Versioned<String>("abcde", version);
        StringSerializer serializer = new StringSerializer();
        byte[] serialized = VectorClockVersionSerializer.toBytes(versioned, serializer);
        assertEquals("The versioned serializes to itself using original protocol.",
                     versioned,
                     VersionFactory.toVersioned(serialized, serializer));
        serialized = VectorClockProtoSerializer.toBytes(versioned, serializer);
        assertEquals("The versioned serializes to itself using protobuf protocol.",
                     versioned,
                     VersionFactory.toVersioned(serialized, serializer));
    }

    protected void compareMerged(Versioned<Integer> one, Versioned<Integer> two) {
        assertEquals("Merged is after",
                     Occured.BEFORE,
                     one.getVersion().compare(one.mergeVersion(two.getVersion()).getVersion()));
        assertEquals("Merged is after",
                     Occured.BEFORE,
                     two.getVersion().compare(one.mergeVersion(two.getVersion()).getVersion()));
        assertEquals("Merged order does not matter",
                     one.mergeVersion(two.getVersion()).getVersion(),
                     two.mergeVersion(one.getVersion()).getVersion());
    }

    @Test
    public void testMerge() {
        // Null versions merge to the same value
        compareMerged(getVersioned(null), getVersioned(null));

        assertEquals("Empty versioneds merge the same.",
                     getVersioned(null),
                     getVersioned(null).mergeVersion(getVersioned(null).getVersion()));

        // Identical versions merge to the same value
        compareMerged(getVersioned(1, 1, 2), getVersioned(1, 1, 2));
        assertEquals("Equal versioneds merge the same.",
                     getVersioned(1, 1, 2),
                     getVersioned(1, 1, 2).mergeVersion(getVersioned(1, 1, 2).getVersion()));

        compareMerged(getVersioned(1, 1, 2), getVersioned(1, 3, 3));

        compareMerged(getVersioned(1, 1, 2), getVersioned(1, 1, 1, 2, 2, 2));
    }
}
