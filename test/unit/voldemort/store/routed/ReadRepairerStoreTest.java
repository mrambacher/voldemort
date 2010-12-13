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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.distributed.AbstractDistributedStoreTest;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedParallelStore;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.distributed.DistributedStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class ReadRepairerStoreTest extends AbstractDistributedStoreTest {

    public ReadRepairerStoreTest() {
        super("read-repair");
    }

    private void awaitCompletion(DistributedStore<?, ?, ?, ?> store) {
        try {
            ReadRepairStore<?, ?, ?, ?> readRepair = (ReadRepairStore<?, ?, ?, ?>) store;
            while((readRepair.pendingRepairs.intValue()) > 0) {
                Thread.sleep(100);
            }

        } catch(Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    private void awaitCompletion(DistributedStore<?, ?, ?, ?> store, StoreFuture<?> future) {
        try {
            DistributedFuture<?, ?> distributed = (DistributedFuture<?, ?>) future;
            distributed.complete();
            awaitCompletion(store);
        } catch(Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }

    @Override
    protected DistributedStore<Node, ByteArray, byte[], byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                                                                      Cluster cluster,
                                                                                      StoreDefinition storeDef,
                                                                                      boolean makeUnique) {
        DistributedStore<Node, ByteArray, byte[], byte[]> distributor = DistributedParallelStore.create(stores,
                                                                                                        storeDef,
                                                                                                        makeUnique);
        return ReadRepairStore.create(distributor);

    }

    private void assertReadRepair(Map<ByteArray, List<Versioned<byte[]>>> values,
                                  Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores) {
        assertReadRepair(values, stores, 0);

    }

    private void assertReadRepair(Map<ByteArray, List<Versioned<byte[]>>> values,
                                  Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                  int expectedFailures) {
        for(ByteArray key: values.keySet()) {
            int failures = 0;
            for(AsynchronousStore<ByteArray, byte[], byte[]> store: stores.values()) {
                try {
                    List<Versioned<byte[]>> expected = values.get(key);
                    List<Versioned<byte[]>> results = store.submitGet(key, null).get();
                    assertEquals("Sizes match for store " + store.getName() + " for key " + key,
                                 expected.size(),
                                 results.size());
                    for(Versioned<byte[]> result: results) {
                        this.assertContains(expected, result);
                    }
                } catch(VoldemortException e) {
                    if(++failures > expectedFailures) {
                        fail("Unexpected exception in store " + store.getName() + " for key " + key
                             + ":" + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * See Issue 150: Missing keys are not added to node when performing
     * read-repair
     */
    @Test
    public void testMissingKeysAreAddedToNodeWhenDoingReadRepair() throws Exception {
        this.createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = createClusterStores(cluster,
                                                                                             "read-repair",
                                                                                             cluster.getNumberOfNodes(),
                                                                                             this.failureDetector,
                                                                                             0,
                                                                                             0,
                                                                                             0,
                                                                                             null);

        StoreDefinition storeDef = getStoreDef("read-repair",
                                               cluster.getNumberOfNodes(),
                                               cluster.getNumberOfNodes(),
                                               cluster.getNumberOfNodes());
        DistributedStore<Node, ByteArray, byte[], byte[]> repair = buildDistributedStore(stores,
                                                                                         cluster,
                                                                                         storeDef);
        AsynchronousStore<ByteArray, byte[], byte[]> async = DistributedStoreFactory.asAsync(storeDef,
                                                                                             repair);

        List<ByteArray> keys = getKeys(3);
        Versioned<byte[]> zero = new Versioned<byte[]>(keys.get(0).get(), TestUtils.getClock(3, 3));
        Versioned<byte[]> one = new Versioned<byte[]>(keys.get(1).get(), TestUtils.getClock(1));
        Versioned<byte[]> two = new Versioned<byte[]>(keys.get(2).get(), TestUtils.getClock(2));

        stores.get(cluster.getNodeById(2)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(3)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(4)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(2)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(3)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(4)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(0)).submitPut(keys.get(2), two, null).get();
        stores.get(cluster.getNodeById(1)).submitPut(keys.get(2), two, null).get();
        stores.get(cluster.getNodeById(2)).submitPut(keys.get(2), two, null).get();

        Map<ByteArray, List<Versioned<byte[]>>> expected = stores.get(cluster.getNodeById(2))
                                                                 .submitGetAll(Collections.singletonList(keys.get(0)),
                                                                               null)
                                                                 .get();

        awaitCompletion(repair, async.submitGet(keys.get(0), null));
        assertReadRepair(expected, stores);

        keys.remove(0);
        expected = stores.get(cluster.getNodeById(2)).submitGetAll(keys, null).get();
        awaitCompletion(repair, async.submitGetAll(keys, null));
        assertReadRepair(expected, stores);
    }

    @Test
    public void testSlowStoreDuringReadRepair() {
        this.createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = createClusterStores(cluster,
                                                                                             "read-repair",
                                                                                             cluster.getNumberOfNodes(),
                                                                                             this.failureDetector,
                                                                                             2,
                                                                                             100,
                                                                                             0,
                                                                                             null);

        StoreDefinition storeDef = getStoreDef("read-repair",
                                               cluster.getNumberOfNodes(),
                                               cluster.getNumberOfNodes(),
                                               cluster.getNumberOfNodes());
        DistributedStore<Node, ByteArray, byte[], byte[]> repair = buildDistributedStore(stores,
                                                                                         cluster,
                                                                                         storeDef);
        AsynchronousStore<ByteArray, byte[], byte[]> async = DistributedStoreFactory.asAsync(storeDef,
                                                                                             repair);

        List<ByteArray> keys = getKeys(3);
        Versioned<byte[]> zero = new Versioned<byte[]>(keys.get(0).get(), TestUtils.getClock(3, 3));
        Versioned<byte[]> one = new Versioned<byte[]>(keys.get(1).get(), TestUtils.getClock(1));
        Versioned<byte[]> two = new Versioned<byte[]>(keys.get(2).get(), TestUtils.getClock(2));

        stores.get(cluster.getNodeById(2)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(3)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(4)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(2)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(3)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(4)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(0)).submitPut(keys.get(2), two, null).get();
        stores.get(cluster.getNodeById(1)).submitPut(keys.get(2), two, null).get();
        stores.get(cluster.getNodeById(2)).submitPut(keys.get(2), two, null).get();

        Map<ByteArray, List<Versioned<byte[]>>> expected = stores.get(cluster.getNodeById(2))
                                                                 .submitGetAll(Collections.singletonList(keys.get(0)),
                                                                               null)
                                                                 .get();

        awaitCompletion(repair, async.submitGet(keys.get(0), null));
        assertReadRepair(expected, stores);

        keys.remove(0);
        expected = stores.get(cluster.getNodeById(2)).submitGetAll(keys, null).get();
        awaitCompletion(repair, async.submitGetAll(keys, null));
        assertReadRepair(expected, stores);
    }

    @Test
    public void testReadRepairWithFailures() {
        this.createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = createClusterStores(cluster,
                                                                                             "read-repair",
                                                                                             cluster.getNumberOfNodes(),
                                                                                             this.failureDetector,
                                                                                             2,
                                                                                             100,
                                                                                             2,
                                                                                             new VoldemortException("no go"));
        StoreDefinition storeDef = getStoreDef("read-repair",
                                               cluster.getNumberOfNodes(),
                                               cluster.getNumberOfNodes() - 2,
                                               cluster.getNumberOfNodes());
        DistributedStore<Node, ByteArray, byte[], byte[]> repair = buildDistributedStore(stores,
                                                                                         cluster,
                                                                                         storeDef);
        AsynchronousStore<ByteArray, byte[], byte[]> async = DistributedStoreFactory.asAsync(storeDef,
                                                                                             repair);

        List<ByteArray> keys = getKeys(3);
        Versioned<byte[]> zero = new Versioned<byte[]>(keys.get(0).get(), TestUtils.getClock(3, 3));
        Versioned<byte[]> one = new Versioned<byte[]>(keys.get(1).get(), TestUtils.getClock(1));
        Versioned<byte[]> two = new Versioned<byte[]>(keys.get(2).get(), TestUtils.getClock(2));

        // 0 + 1 are failed, 2 and 3 are sleepy
        stores.get(cluster.getNodeById(3)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(4)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(5)).submitPut(keys.get(0), zero, null).get();
        stores.get(cluster.getNodeById(4)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(5)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(6)).submitPut(keys.get(1), one, null).get();
        stores.get(cluster.getNodeById(5)).submitPut(keys.get(2), two, null).get();
        stores.get(cluster.getNodeById(6)).submitPut(keys.get(2), two, null).get();
        stores.get(cluster.getNodeById(7)).submitPut(keys.get(2), two, null).get();

        // 5 has all of the keys
        Map<ByteArray, List<Versioned<byte[]>>> expected = stores.get(cluster.getNodeById(5))
                                                                 .submitGetAll(Collections.singletonList(keys.get(0)),
                                                                               null)
                                                                 .get();

        awaitCompletion(repair, async.submitGet(keys.get(0), null));
        assertReadRepair(expected, stores, 2);

        keys.remove(0);
        expected = stores.get(cluster.getNodeById(5)).submitGetAll(keys, null).get();
        awaitCompletion(repair, async.submitGetAll(keys, null));
        assertReadRepair(expected, stores, 2);
    }

    @Test
    public void testReadRepairWithOnlyRequiredResponses() throws Exception {
        this.createFailureDetector();
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores = createClusterStores(cluster,
                                                                                             "read-repair",
                                                                                             cluster.getNumberOfNodes(),
                                                                                             this.failureDetector,
                                                                                             2,
                                                                                             100,
                                                                                             0,
                                                                                             new VoldemortException("oops"));

        StoreDefinition storeDef = getStoreDef("read-repair",
                                               cluster.getNumberOfNodes(),
                                               cluster.getNumberOfNodes() - 2,
                                               cluster.getNumberOfNodes());
        DistributedStore<Node, ByteArray, byte[], byte[]> repair = buildDistributedStore(stores,
                                                                                         cluster,
                                                                                         storeDef);

        ByteArray key = getKey();
        Versioned<byte[]> value = new Versioned<byte[]>(key.get(), TestUtils.getClock(1, 2));

        stores.get(cluster.getNodeById(2)).submitPut(key, value, null).get();
        stores.get(cluster.getNodeById(3)).submitPut(key, value, null).get();
        stores.get(cluster.getNodeById(4)).submitPut(key, value, null).get();

        DistributedFuture<Node, List<Versioned<byte[]>>> f = repair.submitGet(key,
                                                                              null,
                                                                              stores.keySet(),
                                                                              5,
                                                                              3);
        try {
            List<Versioned<byte[]>> expected = f.get(500, TimeUnit.MILLISECONDS);
            Collection<Node> responders = f.getResults().keySet();
            awaitCompletion(repair);
            for(Node node: responders) {
                AsynchronousStore<ByteArray, byte[], byte[]> store = repair.getNodeStore(node);
                List<Versioned<byte[]>> result = store.submitGet(key, null).get();
                assertEquals("Same number of results for " + node, expected.size(), result.size());
                for(Versioned<byte[]> v: expected) {
                    assertContains(result, v);
                }
            }
        } catch(Exception e) {
            fail("Unexpected exception " + e.getMessage());
        }
    }
}
