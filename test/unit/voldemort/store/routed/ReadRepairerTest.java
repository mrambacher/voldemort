/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright (c) 2010 Nokia Corporation. All rights reserved.
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

import static voldemort.TestUtils.getClock;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class ReadRepairerTest extends TestCase {

    private ReadRepairer<Integer, String, Integer> repairer = new ReadRepairer<Integer, String, Integer>();

    private NodeValue<Integer, String, Integer> toValue(int nodeId, int value, Version version) {
        return new NodeValue<Integer, String, Integer>(nodeId,
                                                       Integer.toString(value),
                                                       new Versioned<Integer>(value, version));
    }

    private NodeValue<Integer, String, Integer> getValue(int nodeId, int value, int[] version) {
        return toValue(nodeId, value, getClock(version));
    }

    @Test
    public void testEmptyList() throws Exception {
        assertEquals("Empty list",
                     0,
                     repairer.getRepairs(new ArrayList<NodeValue<Integer, String, Integer>>())
                             .size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSingleValue() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = toList(toValue(1, 1, getClock(1)));
        assertEquals("Empty single list", 0, repairer.getRepairs(nodeValues).size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAllEqual() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(1, 1, getClock(1)),
                                                                  toValue(2, 1, getClock(1)),
                                                                  toValue(3, 1, getClock(1)));
        assertEquals("Empty list", 0, repairer.getRepairs(values).size());
    }

    /**
     * See Issue 92: ReadRepairer.getRepairs should not return duplicates.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testNoDuplicates() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(1, 1, getClock(1, 2)),
                                                                  toValue(2, 1, getClock(1, 2)),
                                                                  toValue(3, 1, getClock(1)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(values);
        this.assertVariationsEqual("No Duplicates", toValue(3, 1, getClock(1, 2)), repairs);
    }

    /**
     * See Issue #211: Unnecessary read repairs during getAll with more than one
     * key
     */
    @Test
    public void testMultipleKeys() {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(0, 1, getClock(2)));
        nodeValues.add(toValue(0, 2, getClock(1)));
        nodeValues.add(toValue(1, 2, getClock(1)));
        nodeValues.add(toValue(1, 1, getClock(2)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("There should be no repairs.", 0, repairs.size());
    }

    boolean checkForExpectedRepair(String message,
                                   NodeValue<Integer, String, Integer> expected,
                                   List<NodeValue<Integer, String, Integer>> repairs) {
        for(NodeValue<Integer, String, Integer> repair: repairs) {
            if(repair.equals(expected)) {
                return true;
            }
        }
        fail(message + ":" + expected);
        return false;
    }

    @Test
    public void testOneNodeOneKeyObsolete() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1, 1)));
        nodeValues.add(toValue(2, 1, getClock(1)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        this.assertVariationsEqual("One node obsolete", toValue(2, 1, getClock(1, 1)), repairs);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testThreeNodesOneKeyObsolete() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1, 1)));
        nodeValues.add(toValue(2, 1, getClock(1)));
        nodeValues.add(toValue(3, 1, getClock(1)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        this.assertVariationsEqual("One key obsolete",
                                   toList(toValue(2, 1, getClock(1, 1)), toValue(3, 1, getClock(1,
                                                                                                1))),
                                   repairs);
    }

    @Test
    public void testThreeNodesOneNodeOneKeyObsolete() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1, 1)));
        nodeValues.add(toValue(3, 1, getClock(1, 1)));
        nodeValues.add(toValue(2, 1, getClock(1)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        this.assertVariationsEqual("One key obsolete", toValue(2, 1, getClock(1, 1)), repairs);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTwoNodesBothObsolete() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(2, 1, getClock(2)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        this.assertVariationsEqual("One key obsolete", toList(toValue(2, 1, getClock(1)),
                                                              toValue(1, 1, getClock(2))), repairs);
    }

    @Test
    public void testNodeMissingVersion() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(2, 1, new VectorClock()));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One key obsolete", toValue(2, 1, getClock(1)), repairs);
    }

    @Test
    public void testCurrentNodesContainObsoleteVersions() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
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

        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("testCurrentNodesContainObsoleteVersions", 0, repairs.size());
    }

    @Test
    public void testNodeMissingOneKey() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(1, 2, getClock(1))); // This key will not get
        // repaired
        nodeValues.add(toValue(2, 1, new VectorClock()));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One repair", toValue(2, 1, getClock(1)), repairs);
    }

    @Test
    public void testThreeNodesOneKeyThreeVersions() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(2, 1, getClock(2)));
        nodeValues.add(toValue(3, 1, getClock(3)));
        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, getClock(2)));
        expected.add(toValue(1, 1, getClock(3)));
        expected.add(toValue(2, 1, getClock(1)));
        expected.add(toValue(2, 1, getClock(3)));
        expected.add(toValue(3, 1, getClock(2)));
        expected.add(toValue(3, 1, getClock(1)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One repair", expected, repairs);
    }

    @Test
    public void testThreeNodesThreeKeyThreeVersions() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1)));
        nodeValues.add(toValue(1, 2, new VectorClock()));
        nodeValues.add(toValue(2, 2, getClock(2)));
        nodeValues.add(toValue(2, 3, new VectorClock()));
        nodeValues.add(toValue(3, 3, getClock(3)));
        nodeValues.add(toValue(3, 1, new VectorClock()));

        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 2, getClock(2)));
        expected.add(toValue(2, 3, getClock(3)));
        expected.add(toValue(3, 1, getClock(1)));

        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("One repair", expected, repairs);
    }

    @Test
    public void testAllConcurrent() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(2)));
        nodeValues.add(toValue(1, 1, getClock(3)));
        nodeValues.add(toValue(2, 1, getClock(1)));
        nodeValues.add(toValue(2, 1, getClock(3)));
        nodeValues.add(toValue(3, 1, getClock(2)));
        nodeValues.add(toValue(3, 1, getClock(1)));

        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, getClock(1)));
        expected.add(toValue(2, 1, getClock(2)));
        expected.add(toValue(3, 1, getClock(3)));

        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);

        assertVariationsEqual("Three repairs", expected, repairs);
    }

    @Test
    public void testTwoAncestorsToOneSuccessor() throws Exception {
        Version clock = getClock(1, 1, 2, 2);
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(2, 1, getClock(1)));
        nodeValues.add(toValue(3, 1, getClock(2)));
        nodeValues.add(toValue(1, 1, clock));
        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(3, 1, clock));
        expected.add(toValue(2, 1, clock));

        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("testTwoAncestorsToOneSuccessor", expected, repairs);
    }

    public void testOneAncestorToTwoSuccessors() throws Exception {
        Version clock = getClock(1, 1, 2, 2);
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(2, 1, clock));
        nodeValues.add(toValue(3, 1, getClock(2)));
        nodeValues.add(toValue(1, 1, clock));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("testOneAcestorToTwoSuccessors", toValue(3, 1, clock), repairs);
    }

    public void testEqualObsoleteVersions() throws Exception {
        Version clock = getClock(1, 1);
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(2, 1, getClock(1)));
        nodeValues.add(toValue(3, 1, getClock(1)));
        nodeValues.add(toValue(1, 1, new VectorClock()));
        nodeValues.add(toValue(4, 1, clock));

        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, clock));
        expected.add(toValue(2, 1, clock));
        expected.add(toValue(3, 1, clock));

        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertVariationsEqual("Obsolete versions", expected, repairs);
    }

    public void testDiamondPattern() throws Exception {
        Version clock = getClock(1, 1, 2, 2);
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, new VectorClock()));
        nodeValues.add(toValue(2, 1, getClock(1)));
        nodeValues.add(toValue(3, 1, getClock(2)));
        nodeValues.add(toValue(4, 1, clock));

        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, clock));
        expected.add(toValue(2, 1, clock));
        expected.add(toValue(3, 1, clock));

        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);

        assertVariationsEqual("Diamond pattern", expected, repairs);
    }

    public void testConcurrentToOneDoesNotImplyConcurrentToAll() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(3, 3)));
        nodeValues.add(toValue(2, 1, getClock(1, 2)));
        nodeValues.add(toValue(3, 1, getClock(1, 3, 3)));

        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, getClock(1, 3, 3)));
        expected.add(toValue(1, 1, getClock(1, 2)));
        expected.add(toValue(2, 1, getClock(1, 3, 3)));
        expected.add(toValue(3, 1, getClock(1, 2)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);

        assertVariationsEqual("Concurrent To One Does Not Imply Concurrent To All",
                              expected,
                              repairs);
    }

    public void testLotsOfVersions() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1, 3)));
        nodeValues.add(toValue(2, 1, getClock(1, 2)));
        nodeValues.add(toValue(3, 1, getClock(2, 2)));
        nodeValues.add(toValue(4, 1, getClock(1, 2, 2, 3)));
        nodeValues.add(toValue(5, 1, getClock(1, 2, 3, 3)));
        nodeValues.add(toValue(6, 1, getClock(3, 3)));

        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, getClock(1, 2, 3, 3)));
        expected.add(toValue(1, 1, getClock(1, 2, 2, 3)));
        expected.add(toValue(2, 1, getClock(1, 2, 2, 3)));
        expected.add(toValue(2, 1, getClock(1, 2, 3, 3)));
        expected.add(toValue(3, 1, getClock(1, 2, 3, 3)));
        expected.add(toValue(3, 1, getClock(1, 2, 2, 3)));
        expected.add(toValue(4, 1, getClock(1, 2, 3, 3)));
        expected.add(toValue(5, 1, getClock(1, 2, 2, 3)));
        expected.add(toValue(6, 1, getClock(1, 2, 2, 3)));
        expected.add(toValue(6, 1, getClock(1, 2, 3, 3)));

        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);

        assertVariationsEqual("Lots of versions", expected, repairs);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleVersionsOnSingleNode() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(1, 1, getClock(1)),
                                                                  toValue(1, 1, getClock(2)),
                                                                  toValue(1, 1, getClock(3)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(values);
        assertEquals("Empty list", 0, repairs.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMultipleKeysOnSingleNode() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(1, 1, getClock(1)),
                                                                  toValue(1, 2, getClock(1)),
                                                                  toValue(1, 3, getClock(1)));
        assertEquals("Empty list", 0, repairer.getRepairs(values).size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAllEqualWithMultipleVersions() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(1, 1, getClock(1)),
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

    @SuppressWarnings("unchecked")
    public void testSingleSuccessor() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(2, 1, getClock(1, 1)),
                                                                  toValue(1, 1, getClock(1)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(values);

        assertVariationsEqual("Single successor", toValue(1, 1, getClock(1, 1)), repairs);

        values = toList(toValue(1, 1, getClock(1)), toValue(2, 1, getClock(1, 1)));
        repairs = repairer.getRepairs(values);
        assertVariationsEqual("Single successor", toValue(1, 1, getClock(1, 1)), repairs);
    }

    public List<NodeValue<Integer, String, Integer>> toList(NodeValue<Integer, String, Integer>... values) {
        List<NodeValue<Integer, String, Integer>> list = new ArrayList<NodeValue<Integer, String, Integer>>(values.length);
        for(int i = 0; i < values.length; i++) {
            list.add(values[i]);
        }
        return list;
    }

    /**
     * Test the equality with a few variations on ordering
     * 
     * @param expected List of expected values
     * @param input List of actual values
     */
    public void assertVariationsEqual(String message,
                                      List<NodeValue<Integer, String, Integer>> expected,
                                      List<NodeValue<Integer, String, Integer>> results) {
        assertEquals("Repairs list matches expected", expected.size(), results.size());
        for(NodeValue<Integer, String, Integer> ex: expected) {
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
                                      NodeValue<Integer, String, Integer> expected,
                                      List<NodeValue<Integer, String, Integer>> results) {
        assertEquals("Repairs list matches expected", 1, results.size());
        checkForExpectedRepair(message, expected, results);
    }

    public void testEqualEqualButNotIdenticalValues() throws Exception {
        ReadRepairer<Integer, String, byte[]> repairer = new ReadRepairer<Integer, String, byte[]>();
        Version clock = getClock(1, 1);
        List<NodeValue<Integer, String, byte[]>> nodeValues = Lists.newArrayList();
        byte[] value1 = new byte[256];
        byte[] value2 = new byte[256];
        for(int i = 0; i < 256; i++) {
            value1[i] = (byte) i;
            value2[i] = (byte) i;
        }
        nodeValues.add(new NodeValue<Integer, String, byte[]>(1,
                                                              "key1",
                                                              new Versioned<byte[]>(value1,
                                                                                    getClock(1, 1))));
        nodeValues.add(new NodeValue<Integer, String, byte[]>(1,
                                                              "key1",
                                                              new Versioned<byte[]>(value2,
                                                                                    getClock(2, 2))));

        nodeValues.add(new NodeValue<Integer, String, byte[]>(2,
                                                              "key1",
                                                              new Versioned<byte[]>(value2,
                                                                                    getClock(1, 1))));
        nodeValues.add(new NodeValue<Integer, String, byte[]>(2,
                                                              "key1",
                                                              new Versioned<byte[]>(value1,
                                                                                    getClock(2, 2))));

        List<NodeValue<Integer, String, Integer>> expected = Lists.newArrayList();
        expected.add(toValue(1, 1, clock));
        expected.add(toValue(2, 1, clock));
        expected.add(toValue(3, 1, clock));

        List<NodeValue<Integer, String, byte[]>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("Empty list", 0, repairs.size());
    }
}
