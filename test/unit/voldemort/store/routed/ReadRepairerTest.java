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

package voldemort.store.routed;

import static java.util.Arrays.asList;
import static voldemort.TestUtils.getClock;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * 
 */
@SuppressWarnings("unchecked")
public class ReadRepairerTest extends TestCase {

    private ReadRepairer<String, Integer> repairer = new ReadRepairer<String, Integer>();
    private List<NodeValue<String, Integer>> empty = new ArrayList<NodeValue<String, Integer>>();

    @Test
    public void testEmptyList() throws Exception {
        assertEquals(empty, repairer.getRepairs(empty));
    }

    @Test
    public void testSingleValue() throws Exception {
        assertEquals(empty, repairer.getRepairs(asList(getValue(1, 1, new int[] { 1 }))));
    }

    @Test
    public void testAllEqual() throws Exception {
        List<NodeValue<String, Integer>> values = asList(getValue(1, 1, new int[] { 1 }),
                                                         getValue(2, 1, new int[] { 1 }),
                                                         getValue(3, 1, new int[] { 1 }));
        assertEquals(empty, repairer.getRepairs(values));
    }

    /**
     * See Issue 92: ReadRepairer.getRepairs should not return duplicates.
     */
    public void testNoDuplicates() throws Exception {
        List<NodeValue<String, Integer>> values = asList(getValue(1, 1, new int[] { 1, 2 }),
                                                         getValue(2, 1, new int[] { 1, 2 }),
                                                         getValue(3, 1, new int[] { 1 }));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(values);
        assertEquals(1, repairs.size());
        assertEquals(getValue(3, 1, new int[] { 1, 2 }), repairs.get(0));
    }

    public void testSingleSuccessor() throws Exception {
        assertVariationsEqual("Test single successor",
                              getValue(1, 1, new int[] { 1, 1 }),
                              asList(getValue(1, 1, new int[] { 1 }), getValue(2, 1, new int[] { 1,
                                      1 })));
    }

    public void testAllConcurrent() throws Exception {
        assertVariationsEqual("Test all concurrent",
                              asList(getValue(1, 1, new int[] { 2 }),
                                     getValue(1, 1, new int[] { 3 }),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(2, 1, new int[] { 3 }),
                                     getValue(3, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })),
                              asList(getValue(1, 1, new int[] { 1 }),
                                     getValue(2, 1, new int[] { 2 }),
                                     getValue(3, 1, new int[] { 3 })));
    }

    public void testTwoAncestorsToOneSuccessor() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual("testTwoAncestorsToOneSuccessor",
                              asList(getValue(2, 1, expected), getValue(3, 1, expected)),
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })));
    }

    public void testOneAcestorToTwoSuccessors() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual("testOneAcestorToTwoSuccessors",
                              asList(getValue(2, 1, expected), getValue(3, 1, expected)),
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })));
    }

    public void testEqualObsoleteVersions() throws Exception {
        int[] expected = new int[] { 1, 1 };
        assertVariationsEqual("testEqualObsoleteVersions",
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, expected),
                                     getValue(3, 1, expected)),
                              asList(getValue(1, 1, new int[] {}),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 1 }),
                                     getValue(4, 1, expected)));
    }

    public void testDiamondPattern() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual("testDiamondPattern",
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, expected),
                                     getValue(3, 1, expected)),
                              asList(getValue(1, 1, new int[] {}),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 }),
                                     getValue(4, 1, expected)));
    }

    public void testConcurrentToOneDoesNotImplyConcurrentToAll() throws Exception {
        assertVariationsEqual("testConcurrentToOneDoesNotImplyConcurrentToAll",
                              asList(getValue(1, 1, new int[] { 1, 3, 3 }),
                                     getValue(1, 1, new int[] { 1, 2 }),
                                     getValue(2, 1, new int[] { 1, 3, 3 }),
                                     getValue(3, 1, new int[] { 1, 2 })),
                              asList(getValue(1, 1, new int[] { 3, 3 }), getValue(2, 1, new int[] {
                                      1, 2 }), getValue(3, 1, new int[] { 1, 3, 3 })));
    }

    public void testLotsOfVersions() throws Exception {
        assertVariationsEqual("testLotsOfVersions",
                              asList(getValue(1, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(1, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(2, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(2, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(3, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(3, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(4, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(5, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(6, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(6, 1, new int[] { 1, 2, 3, 3 })),
                              asList(getValue(1, 1, new int[] { 1, 3 }),
                                     getValue(2, 1, new int[] { 1, 2 }),
                                     getValue(3, 1, new int[] { 2, 2 }),
                                     getValue(4, 1, new int[] { 1, 2, 2, 3 }),
                                     getValue(5, 1, new int[] { 1, 2, 3, 3 }),
                                     getValue(6, 1, new int[] { 3, 3 })));
    }

    /**
     * See Issue #211: Unnecessary read repairs during getAll with more than one
     * key
     */
    @Test
    public void testMultipleKeys() {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(getValue(0, 1, new int[2]));
        nodeValues.add(getValue(0, 2, new int[0]));
        nodeValues.add(getValue(1, 2, new int[0]));
        nodeValues.add(getValue(2, 1, new int[2]));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("There should be no repairs.", 0, repairs.size());
    }

    @Test
    public void testMultipleKeysOnSingleNode() throws Exception {
        List<NodeValue<String, Integer>> values = asList(toValue(1, 1, getClock(1)),
                                                         toValue(1, 2, getClock(1)),
                                                         toValue(1, 3, getClock(1)));
        assertEquals("Empty list", 0, repairer.getRepairs(values).size());
    }

    @Test
    public void testAllEqualWithMultipleVersions() throws Exception {
        List<NodeValue<String, Integer>> values = asList(toValue(1, 1, getClock(1)),
                                                         toValue(1, 1, getClock(2)),
                                                         toValue(1, 1, getClock(3)),
                                                         toValue(2, 1, getClock(1)),
                                                         toValue(2, 1, getClock(2)),
                                                         toValue(2, 1, getClock(3)),
                                                         toValue(3, 1, getClock(1)),
                                                         toValue(3, 1, getClock(2)),
                                                         toValue(3, 1, getClock(3)));
        assertEquals("Empty list", 0, repairer.getRepairs(values).size());
    }

    @Test
    public void testCurrentNodesContainObsoleteVersions() throws Exception {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(1, 1, getClock(1, 1)));
        nodeValues.add(toValue(1, 1, getClock(2, 2)));

        nodeValues.add(toValue(1, 2, getClock(1)));
        nodeValues.add(toValue(1, 2, getClock(1, 1)));
        nodeValues.add(toValue(1, 2, getClock(2, 2)));

        nodeValues.add(toValue(2, 1, getClock(1, 1)));
        nodeValues.add(toValue(2, 1, getClock(2)));
        nodeValues.add(toValue(2, 1, getClock(2, 2)));

        nodeValues.add(toValue(2, 2, getClock(1, 1)));
        nodeValues.add(toValue(2, 2, getClock(2)));
        nodeValues.add(toValue(2, 2, getClock(2, 2)));

        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("testCurrentNodesContainObsoleteVersions", 0, repairs.size());
    }

    @Test
    public void testNodeMissingOneKey() throws Exception {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(1, 2, getClock(1))); // This key will not get
        // repaired
        nodeValues.add(toValue(2, 1, new VectorClock()));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One repair", toValue(2, 1, getClock(1)), repairs);
    }

    @Test
    public void testMultipleVersionsOnSingleNode() throws Exception {
        List<NodeValue<String, Integer>> values = asList(toValue(1, 1, getClock(1)),
                                                         toValue(1, 1, getClock(2)),
                                                         toValue(1, 1, getClock(3)));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(values);
        assertEquals("Empty list", 0, repairs.size());
    }

    @Test
    public void testThreeNodesOneKeyThreeVersions() throws Exception {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(2, 1, getClock(2)));
        nodeValues.add(toValue(3, 1, getClock(3)));
        List<NodeValue<String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, getClock(2)));
        expected.add(toValue(1, 1, getClock(3)));
        expected.add(toValue(2, 1, getClock(1)));
        expected.add(toValue(2, 1, getClock(3)));
        expected.add(toValue(3, 1, getClock(2)));
        expected.add(toValue(3, 1, getClock(1)));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One repair", expected, repairs);
    }

    @Test
    public void testThreeNodesThreeKeyThreeVersions() throws Exception {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(1, 2, new VectorClock()));
        nodeValues.add(toValue(2, 2, getClock(2)));
        nodeValues.add(toValue(2, 3, new VectorClock()));
        nodeValues.add(toValue(3, 3, getClock(3)));
        nodeValues.add(toValue(3, 1, new VectorClock()));

        List<NodeValue<String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 2, getClock(2)));
        expected.add(toValue(2, 3, getClock(3)));
        expected.add(toValue(3, 1, getClock(1)));

        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One repair", expected, repairs);
    }

    @Test
    public void testNodeMissingVersion() throws Exception {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(2, 1, new VectorClock()));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One key obsolete", toValue(2, 1, getClock(1)), repairs);
    }

    @Test
    public void testOneNodeOneKeyObsolete() throws Exception {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1, 1)));
        nodeValues.add(toValue(2, 1, getClock(1)));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        this.assertVariationsEqual("One node obsolete", toValue(2, 1, getClock(1, 1)), repairs);
    }

    @Test
    public void testTwoNodesBothObsolete() throws Exception {
        List<NodeValue<String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(2, 1, getClock(2)));
        List<NodeValue<String, Integer>> repairs = repairer.getRepairs(nodeValues);
        this.assertVariationsEqual("One key obsolete", asList(toValue(2, 1, getClock(1)),
                                                              toValue(1, 1, getClock(2))), repairs);
    }

    private NodeValue<String, Integer> getValue(int nodeId, int value, int[] version) {
        return toValue(nodeId, value, getClock(version));
    }

    private NodeValue<String, Integer> toValue(int nodeId, int value, Version version) {
        return new NodeValue<String, Integer>(nodeId,
                                              Integer.toString(value),
                                              new Versioned<Integer>(value, version));
    }

    boolean checkForExpectedRepair(String message,
                                   NodeValue<String, Integer> expected,
                                   List<NodeValue<String, Integer>> repairs) {
        for(NodeValue<String, Integer> repair: repairs) {
            if(repair.equals(expected)) {
                return true;
            }
        }
        fail(message + ":" + expected);
        return false;
    }

    /**
     * Test the equality with a few variations on ordering
     * 
     * @param expected List of expected values
     * @param input List of actual values
     */
    public void assertVariationsEqual(String message,
                                      List<NodeValue<String, Integer>> expected,
                                      List<NodeValue<String, Integer>> results) {
        assertEquals("Repairs list matches expected", expected.size(), results.size());
        for(NodeValue<String, Integer> ex: expected) {
            checkForExpectedRepair(message, ex, results);
        }
    }

    /**
     * Test the equality with a few variations on ordering
     * 
     * @param expected List of expected values
     * @param input List of actual values
     */
    public void assertVariationsEqual(String message,
                                      NodeValue<String, Integer> expected,
                                      List<NodeValue<String, Integer>> results) {
        assertEquals("Repairs list matches expected", 1, results.size());
        checkForExpectedRepair(message, expected, results);
    }

    public void testEqualEqualButNotIdenticalValues() throws Exception {
        ReadRepairer<String, byte[]> repairer = new ReadRepairer<String, byte[]>();
        Version clock = getClock(1, 1);
        List<NodeValue<String, byte[]>> nodeValues = Lists.newArrayList();
        byte[] value1 = new byte[256];
        byte[] value2 = new byte[256];
        for(int i = 0; i < 256; i++) {
            value1[i] = (byte) i;
            value2[i] = (byte) i;
        }
        nodeValues.add(new NodeValue<String, byte[]>(1,
                                                     "key1",
                                                     new Versioned<byte[]>(value1, getClock(1, 1))));
        nodeValues.add(new NodeValue<String, byte[]>(1,
                                                     "key1",
                                                     new Versioned<byte[]>(value2, getClock(2, 2))));

        nodeValues.add(new NodeValue<String, byte[]>(2,
                                                     "key1",
                                                     new Versioned<byte[]>(value2, getClock(1, 1))));
        nodeValues.add(new NodeValue<String, byte[]>(2,
                                                     "key1",
                                                     new Versioned<byte[]>(value1, getClock(2, 2))));

        List<NodeValue<String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, clock));
        expected.add(toValue(2, 1, clock));
        expected.add(toValue(3, 1, clock));

        List<NodeValue<String, byte[]>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("Empty list", 0, repairs.size());
    }
}
