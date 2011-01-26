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

package voldemort.store.invalidmetadata;

import java.util.Map;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.DoNothingStore;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

/**
 * 
 */
public class InvalidMetadataCheckingStoreTest extends TestCase {

    public void testValidMetadata() {
        Cluster cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, { 8, 9, 10, 11 } });
        StoreDefinition storeDef = ServerTestUtils.getStoreDefs(1).get(0);
        MetadataStore metadataStore = ServerTestUtils.createMetadataStore(cluster, storeDef);
        RoutingStrategy strategy = metadataStore.getRoutingStrategy(storeDef.getName());
        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<ByteArray, byte[], byte[]>(storeDef),
                                                                              metadataStore);
        this.testOperation(cluster.getNodeById(0),
                           metadataStore,
                           store,
                           TestUtils.getKeysForPartitions(strategy, cluster.getNodeById(0)
                                                                           .getPartitionIds()));
    }

    public void testInvalidMetadata() {
        Cluster cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, { 8, 9, 10, 11 } });
        StoreDefinition storeDef = ServerTestUtils.getStoreDefs(1).get(0);
        MetadataStore metadataStore = ServerTestUtils.createMetadataStore(cluster, storeDef);
        RoutingStrategy strategy = metadataStore.getRoutingStrategy(storeDef.getName());
        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<ByteArray, byte[], byte[]>(storeDef),
                                                                              metadataStore);
        this.testOperation(cluster.getNodeById(0),
                           strategy,
                           store,
                           TestUtils.getKeysForPartitions(strategy, cluster.getNodeById(1)
                                                                           .getPartitionIds()));
        this.testOperation(cluster.getNodeById(0),
                           strategy,
                           store,
                           TestUtils.getKeysForPartitions(strategy, cluster.getNodeById(2)
                                                                           .getPartitionIds()));
    }

    /**
     * NOTE: the total number of partitions should remain same for hash
     * consistency
     */
    public void testAddingPartition() {
        StoreDefinition storeDef = ServerTestUtils.getStoreDefs(1).get(0);

        Cluster cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, { 8, 9, 10 } });

        MetadataStore metadataStore = ServerTestUtils.createMetadataStore(cluster, storeDef);
        Map<Integer, ByteArray> keys = TestUtils.getKeysForPartitions(metadataStore.getRoutingStrategy(storeDef.getName()),
                                                                      cluster.getNumberOfPartitions());
        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<ByteArray, byte[], byte[]>(storeDef),
                                                                              metadataStore);
        this.testOperation(cluster.getNodeById(0), metadataStore, store, keys);
        // add partitions to node 0 on client side.
        Cluster updated = ServerTestUtils.getLocalCluster(3, new int[][] {
                { 0, 1, 2, 3, 4, 5, 10 }, { 6, 7 }, { 8, 9 } });
        metadataStore.put(MetadataStore.CLUSTER_KEY, updated);
        this.testOperation(updated.getNodeById(0), metadataStore, store, keys);
    }

    public void testRemovingPartition() {
        StoreDefinition storeDef = ServerTestUtils.getStoreDefs(1).get(0);

        Cluster cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 },
                { 4, 5, 6, 7 }, { 8, 9, 10 } });
        MetadataStore metadataStore = ServerTestUtils.createMetadataStore(cluster, storeDef);
        Map<Integer, ByteArray> keys = TestUtils.getKeysForPartitions(metadataStore.getRoutingStrategy(storeDef.getName()),
                                                                      cluster.getNumberOfPartitions());

        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<ByteArray, byte[], byte[]>(storeDef),
                                                                              metadataStore);
        this.testOperation(cluster.getNodeById(0), metadataStore, store, keys);

        // remove partitions to node 0 on client side.
        Cluster updated = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1 },
                { 2, 4, 5, 6, 7 }, { 3, 8, 9, 10 } });
        metadataStore.put(MetadataStore.CLUSTER_KEY, updated);
        this.testOperation(updated.getNodeById(0), metadataStore, store, keys);
    }

    private void testOperation(Node node,
                               MetadataStore metadata,
                               Store<ByteArray, byte[], byte[]> store,
                               Map<Integer, ByteArray> keys) {
        testOperation(node, metadata.getRoutingStrategy(store.getName()), store, keys);
    }

    private void testOperation(Node node,
                               RoutingStrategy strategy,
                               Store<ByteArray, byte[], byte[]> store,
                               Map<Integer, ByteArray> keys) {
        for(ByteArray key: keys.values()) {
            try {
                store.get(key, null);
                assertTrue(strategy.routeRequest(key.get()).contains(node));
            } catch(VoldemortException ex) {
                assertEquals("Unexpected Exception", InvalidMetadataException.class, ex.getClass());
                assertFalse(strategy.routeRequest(key.get()).contains(node));
            }
            try {
                store.delete(key, VersionFactory.newVersion());
                assertTrue(strategy.routeRequest(key.get()).contains(node));
            } catch(VoldemortException ex) {
                assertEquals("Unexpected Exception", InvalidMetadataException.class, ex.getClass());
                assertFalse(strategy.routeRequest(key.get()).contains(node));
            }
            try {
                store.put(key, new Versioned<byte[]>(key.get()), null);
                assertTrue(strategy.routeRequest(key.get()).contains(node));
            } catch(VoldemortException ex) {
                assertEquals("Unexpected Exception", InvalidMetadataException.class, ex.getClass());
                assertFalse(strategy.routeRequest(key.get()).contains(node));
            }
            try {
                store.getAll(ImmutableList.of(key), null);
                assertTrue(strategy.routeRequest(key.get()).contains(node));
            } catch(VoldemortException ex) {
                assertEquals("Unexpected Exception", InvalidMetadataException.class, ex.getClass());
                assertFalse(strategy.routeRequest(key.get()).contains(node));
            }
        }
    }
}
