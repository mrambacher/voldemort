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

import static java.util.Arrays.asList;
import static voldemort.FailureDetectorTestUtils.recordException;
import static voldemort.FailureDetectorTestUtils.recordSuccess;
import static voldemort.MutableStoreVerifier.create;
import static voldemort.TestUtils.getClock;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class ReadRepairerTest extends TestCase {

    private ReadRepairer<Integer, String, Integer> repairer = new ReadRepairer<Integer, String, Integer>();
    private List<NodeValue<Integer, String, Integer>> empty = new ArrayList<NodeValue<Integer, String, Integer>>();
    private Random random = new Random(1456);

    private Time time = new MockTime();
    private final Class<FailureDetector> failureDetectorClass;
    private FailureDetector failureDetector;

    public ReadRepairerTest(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class } });
    }

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
        List<NodeValue<Integer, String, Integer>> values = asList(getValue(1, 1, new int[] { 1 }),
                                                                  getValue(2, 1, new int[] { 1 }),
                                                                  getValue(3, 1, new int[] { 1 }));
        assertEquals(empty, repairer.getRepairs(values));
    }

    /**
     * See Issue 150: Missing keys are not added to node when performing
     * read-repair
     */

    @Test
    public void testMissingKeysAreAddedToNodeWhenDoingReadRepair() throws Exception {
        ByteArray key = TestUtils.toByteArray("key");
        byte[] value = "foo".getBytes();

        Cluster cluster = VoldemortTestConstants.getThreeNodeCluster();
        StoreDefinition storeDef = ServerTestUtils.getStoreDef("test",
                                                               3,
                                                               3,
                                                               3,
                                                               2,
                                                               2,
                                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setBannagePeriod(1000)
                                                                                 .setNodes(cluster.getNodes())
                                                                                 .setTime(time);

        failureDetector = create(failureDetectorConfig, false);
        Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
        for(int a = 0; a < 3; a++) {
            Node node = Iterables.get(cluster.getNodes(), a);
            subStores.put(node.getId(),
                          new NodeStore<ByteArray, byte[]>(node,
                                                           failureDetector,
                                                           new InMemoryStorageEngine<ByteArray, byte[]>("test")));
        }
        failureDetector.getConfig().setStoreVerifier(create(subStores));

        ReadRepairStore<Integer> store = new ReadRepairStore<Integer>("test",
                                                                      subStores,
                                                                      storeDef,
                                                                      1,
                                                                      1000L,
                                                                      TimeUnit.MILLISECONDS,
                                                                      true);

        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        Versioned<byte[]> versioned = new Versioned<byte[]>(value);
        versioned.getVersion().incrementClock(1);
        store.put(key, versioned);
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        time.sleep(2000);

        assertEquals(2, store.get(key).size());
        // Last get should have repaired the missing key from node 0 so all
        // stores should now return a value
        assertEquals(3, store.get(key).size());

        ByteArray anotherKey = TestUtils.toByteArray("anotherKey");
        // Try again, now use getAll to read repair
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 0));
        store.put(anotherKey, versioned);
        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 0));
        assertEquals(2, store.getAll(Arrays.asList(anotherKey)).get(anotherKey).size());
        assertEquals(3, store.get(anotherKey).size());
    }

    /**
     * See Issue 92: ReadRepairer.getRepairs should not return duplicates.
     */
    public void testNoDuplicates() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = asList(getValue(1, 1, new int[] { 1, 2 }),
                                                                  getValue(2, 1, new int[] { 1, 2 }),
                                                                  getValue(3, 1, new int[] { 1 }));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(values);
        assertEquals(1, repairs.size());
        assertEquals(getValue(3, 1, new int[] { 1, 2 }), repairs.get(0));
    }

    /**
     * Test the equality with a few variations on ordering
     * 
     * @param expected List of expected values
     * @param input List of actual values
     */
    public void assertVariationsEqual(List<NodeValue<Integer, String, Integer>> expected,
                                      List<NodeValue<Integer, String, Integer>> input) {
        List<NodeValue<Integer, String, Integer>> copy = new ArrayList<NodeValue<Integer, String, Integer>>(input);
        for(int i = 0; i < Math.min(5, copy.size()); i++) {
            int j = random.nextInt(copy.size());
            int k = random.nextInt(copy.size());
            Collections.swap(copy, j, k);
            Set<NodeValue<Integer, String, Integer>> expSet = Sets.newHashSet(expected);
            List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(copy);
            Set<NodeValue<Integer, String, Integer>> repairSet = Sets.newHashSet(repairs);
            assertEquals("Repairs list contains duplicates on iteration" + i + ".",
                         repairs.size(),
                         repairSet.size());
            assertEquals("Expected repairs do not equal found repairs on iteration " + i + " : ",
                         expSet,
                         repairSet);
        }
    }

    /**
     * See Issue #211: Unnecessary read repairs during getAll with more than one
     * key
     */
    @Test
    public void testMultipleKeys() {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(getValue(0, 1, new int[2]));
        nodeValues.add(getValue(0, 2, new int[0]));
        nodeValues.add(getValue(1, 2, new int[0]));
        nodeValues.add(getValue(2, 1, new int[2]));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        assertEquals("There should be no repairs.", 0, repairs.size());
    }

    private NodeValue<Integer, String, Integer> getValue(int nodeId, int value, int[] version) {
        return toValue(nodeId, value, getClock(version));
    }

    private NodeValue<Integer, String, Integer> toValue(int nodeId, int value, Version version) {
        return new NodeValue<Integer, String, Integer>(nodeId,
                                                       Integer.toString(value),
                                                       new Versioned<Integer>(value, version));
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

    @Test
    public void testEqualEqualButNotIdenticalValues() throws Exception {
        ReadRepairer<Integer, String, byte[]> repairer = new ReadRepairer<Integer, String, byte[]>();
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

        assertEquals("Empty list", 0, repairer.getRepairs(nodeValues).size());
    }

    @Test
    public void testOneNodeOneKeyObsolete() throws Exception {
        List<NodeValue<Integer, String, Integer>> nodeValues = Lists.newArrayList();
        nodeValues.add(toValue(1, 1, getClock(1, 1)));
        nodeValues.add(toValue(2, 1, getClock(1)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(nodeValues);
        this.assertVariationsEqual("One node obsolete", toValue(2, 1, getClock(1, 1)), repairs);
    }

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
    public void testMultipleVersionsOnSingleNode() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(1, 1, getClock(1)),
                                                                  toValue(1, 1, getClock(2)),
                                                                  toValue(1, 1, getClock(3)));
        List<NodeValue<Integer, String, Integer>> repairs = repairer.getRepairs(values);
        assertEquals("Empty list", 0, repairs.size());
    }

    @Test
    public void testMultipleKeysOnSingleNode() throws Exception {
        List<NodeValue<Integer, String, Integer>> values = toList(toValue(1, 1, getClock(1)),
                                                                  toValue(1, 2, getClock(1)),
                                                                  toValue(1, 3, getClock(1)));
        assertEquals("Empty list", 0, repairer.getRepairs(values).size());
    }

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

    @Test
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
}
