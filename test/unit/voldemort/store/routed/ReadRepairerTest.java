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
import static java.util.Collections.singletonList;
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

    public void testSingleSuccessor() throws Exception {
        assertVariationsEqual(singletonList(getValue(1, 1, new int[] { 1, 1 })),
                              asList(getValue(1, 1, new int[] { 1 }), getValue(2, 1, new int[] { 1,
                                      1 })));
    }

    public void testAllConcurrent() throws Exception {
        assertVariationsEqual(asList(getValue(1, 1, new int[] { 2 }),
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
        assertVariationsEqual(asList(getValue(2, 1, expected), getValue(3, 1, expected)),
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })));
    }

    public void testOneAcestorToTwoSuccessors() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual(asList(getValue(2, 1, expected), getValue(3, 1, expected)),
                              asList(getValue(1, 1, expected),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 })));
    }

    public void testEqualObsoleteVersions() throws Exception {
        int[] expected = new int[] { 1, 1 };
        assertVariationsEqual(asList(getValue(1, 1, expected),
                                     getValue(2, 1, expected),
                                     getValue(3, 1, expected)),
                              asList(getValue(1, 1, new int[] {}),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 1 }),
                                     getValue(4, 1, expected)));
    }

    public void testDiamondPattern() throws Exception {
        int[] expected = new int[] { 1, 1, 2, 2 };
        assertVariationsEqual(asList(getValue(1, 1, expected),
                                     getValue(2, 1, expected),
                                     getValue(3, 1, expected)),
                              asList(getValue(1, 1, new int[] {}),
                                     getValue(2, 1, new int[] { 1 }),
                                     getValue(3, 1, new int[] { 2 }),
                                     getValue(4, 1, expected)));
    }

    public void testConcurrentToOneDoesNotImplyConcurrentToAll() throws Exception {
        assertVariationsEqual(asList(getValue(1, 1, new int[] { 1, 3, 3 }),
                                     getValue(1, 1, new int[] { 1, 2 }),
                                     getValue(2, 1, new int[] { 1, 3, 3 }),
                                     getValue(3, 1, new int[] { 1, 2 })),
                              asList(getValue(1, 1, new int[] { 3, 3 }), getValue(2, 1, new int[] {
                                      1, 2 }), getValue(3, 1, new int[] { 1, 3, 3 })));
    }

    public void testLotsOfVersions() throws Exception {
        assertVariationsEqual(asList(getValue(1, 1, new int[] { 1, 2, 2, 3 }),
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
}
