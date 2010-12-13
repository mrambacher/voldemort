/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.store.rebalancing;

import java.util.ArrayList;
import java.util.Map;

import voldemort.client.DefaultStoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.async.AsyncUtils;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.failuredetector.FailureDetectingStore;
import voldemort.store.invalidmetadata.MetadataCheckingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Versioned;

/**
 * The RebootstrappingStore catch all InvalidMetadataException and updates the
 * routed store with latest cluster metadata, client rebootstrapping behavior
 * same in {@link DefaultStoreClient} for server side routing<br>
 * 
 */
public class RebootstrappingStore extends MetadataCheckingStore<ByteArray, byte[], byte[]> {

    private final static int maxMetadataRefreshAttempts = 3;

    private final MetadataStore metadata;
    private final StoreRepository storeRepository;
    private final VoldemortConfig voldemortConfig;
    private final RoutedStore routedStore;
    private final SocketStoreFactory storeFactory;
    private final FailureDetector failureDetector;

    public RebootstrappingStore(MetadataStore metadata,
                                StoreRepository storeRepository,
                                VoldemortConfig voldemortConfig,
                                RoutedStore routedStore,
                                SocketStoreFactory storeFactory,
                                FailureDetector failureDetector) {
        super(routedStore.getName(), maxMetadataRefreshAttempts, routedStore);
        this.metadata = metadata;
        this.storeRepository = storeRepository;
        this.voldemortConfig = voldemortConfig;
        this.routedStore = routedStore;
        this.storeFactory = storeFactory;
        this.failureDetector = failureDetector;
    }

    @Override
    protected void reinit() {
        AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                       metadata.getCluster(),
                                                                       4,
                                                                       2);
        try {
            Versioned<Cluster> latestCluster = RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                               adminClient);
            metadata.put(MetadataStore.CLUSTER_KEY, latestCluster.getValue());

            checkAndAddNodeStore();

            routedStore.updateRoutingStrategy(metadata.getRoutingStrategy(getName()));
        } finally {
            adminClient.stop();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> getNodeStores() {
        return (Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>>) routedStore.getCapability(StoreCapabilityType.NODE_STORES);
    }

    /**
     * Check that all nodes in the new cluster have a corrosponding entry in
     * storeRepositiry and innerStores. add a NodeStore if not present, is
     * needed as with rebalancing we can add new nodes on the fly.
     * 
     */
    private void checkAndAddNodeStore() {
        Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> nodeStores = getNodeStores();
        for(Node node: metadata.getCluster().getNodes()) {
            if(!routedStore.getNodeStores().containsKey(node.getId())) {
                if(!storeRepository.hasNodeStore(getName(), node.getId())) {
                    storeRepository.addNodeStore(node.getId(), createNodeStore(node));
                }
                nodeStores.put(node, AsyncUtils.asAsync(storeRepository.getNodeStore(getName(),
                                                                                     node.getId())));
            }
        }
    }

    private Store<ByteArray, byte[], byte[]> createNodeStore(Node node) {
        AsynchronousStore<ByteArray, byte[], byte[]> async = storeFactory.create(getName(),
                                                                                 node.getHost(),
                                                                                 node.getSocketPort(),
                                                                                 voldemortConfig.getRequestFormatType(),
                                                                                 RequestRoutingType.NORMAL);
        return AsyncUtils.asStore(FailureDetectingStore.create(node, failureDetector, async));
    }

}
