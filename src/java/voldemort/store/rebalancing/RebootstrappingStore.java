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

package voldemort.store.rebalancing;

import java.util.ArrayList;

import voldemort.client.DefaultStoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.invalidmetadata.MetadataCheckingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Versioned;

/**
 * The RebootstrappingStore catch all InvalidMetadataException and updates the
 * routed store with latest cluster metadata, client rebootstrapping behavior
 * same in {@link DefaultStoreClient} for server side routing<br>
 * 
 */
public class RebootstrappingStore extends MetadataCheckingStore<ByteArray, byte[]> {

    private final static int maxMetadataRefreshAttempts = 3;

    private final MetadataStore metadata;
    private final StoreRepository storeRepository;
    private final VoldemortConfig voldemortConfig;
    private final SocketPool socketPool;
    private RoutedStore routedStore;

    public RebootstrappingStore(MetadataStore metadataStore,
                                StoreRepository storeRepository,
                                VoldemortConfig voldemortConfig,
                                SocketPool socketPool,
                                RoutedStore routedStore) {
        super(routedStore.getName(), maxMetadataRefreshAttempts, routedStore);
        this.metadata = metadataStore;
        this.storeRepository = storeRepository;
        this.voldemortConfig = voldemortConfig;
        this.socketPool = socketPool;
        this.routedStore = routedStore;
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

    /**
     * Check that all nodes in the new cluster have a corresponding entry in
     * storeRepository and innerStores. add a NodeStore if not present, is
     * needed as with rebalancing we can add new nodes on the fly.
     * 
     */
    private void checkAndAddNodeStore() {

        for(Node node: metadata.getCluster().getNodes()) {
            if(!routedStore.getInnerStores().containsKey(node)) {
                if(!storeRepository.hasNodeStore(getName(), node.getId())) {
                    storeRepository.addNodeStore(node.getId(), createNodeStore(node));
                }
                routedStore.getInnerStores().put(node.getId(),
                                                 storeRepository.getNodeStore(getName(),
                                                                              node.getId()));
            }
        }
    }

    private Store<ByteArray, byte[]> createNodeStore(Node node) {
        return new SocketStore(getName(),
                               new SocketDestination(node.getHost(),
                                                     node.getSocketPort(),
                                                     voldemortConfig.getRequestFormatType()),
                               socketPool,
                               false);
    }
}
