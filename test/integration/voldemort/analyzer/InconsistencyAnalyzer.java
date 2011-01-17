/*
 * Copyright 2010 Nokia Corporation. All rights reserved.
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
package voldemort.analyzer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Version;

public class InconsistencyAnalyzer {

    public static void main(String[] args) throws Exception {
        if(args.length < 1 || args.length > 2)
            Utils.croak("USAGE: java InconsistencyCounter bootstrap_url store");

        String bootstrapUrl = args[0];
        String storeName = args[1];
        AdminClient adminClient = null;
        Store<ByteArray, byte[], byte[]> clientStore = null;
        RoutingStrategy routingStrategy = null;
        Map<Node, Store<ByteArray, byte[], byte[]>> nodeStores = null;
        int keyCount = 0;
        int errCount = 0;
        int inconsistencies = 0;
        int repairs = 0;
        boolean printReadRepair = false;
        boolean printInconsistencies = true;
        try {
            adminClient = new AdminClient(bootstrapUrl, new AdminClientConfig());
        } catch(Exception e) {
            Utils.croak("Couldn't instantiate admin client: " + e.getMessage());
        }
        try {
            clientStore = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)).getRawStore(storeName,
                                                                                                                      null);
            nodeStores = (Map<Node, Store<ByteArray, byte[], byte[]>>) clientStore.getCapability(StoreCapabilityType.SYNCHRONOUS_NODE_STORES);
            routingStrategy = (RoutingStrategy) clientStore.getCapability(StoreCapabilityType.ROUTING_STRATEGY);
        } catch(Exception e) {
            Utils.croak("Couldn't instantiate store: " + e.getMessage());
        }

        System.out.println("Created store client for store " + storeName);

        Cluster cluster = adminClient.getAdminClientCluster();
        for(int partition = 0; partition < cluster.getNumberOfPartitions(); partition++) {
            List<Integer> partitionList = new ArrayList<Integer>(1);
            partitionList.add(partition);
            System.out.println("Retrieving keys for partition " + partition);
            Set<ByteArray> keys = new HashSet<ByteArray>();
            for(Node node: cluster.getNodes()) {
                int count = 0;
                try {
                    Iterator<ByteArray> nodeKeys = adminClient.fetchKeys(node.getId(),
                                                                         storeName,
                                                                         partitionList,
                                                                         null,
                                                                         false);
                    while(nodeKeys.hasNext()) {
                        ByteArray key = nodeKeys.next();
                        if(routingStrategy.getPrimaryPartition(key.get()) != partition) {
                            System.out.println("Key [" + new String(key.get()) + "] on " + node
                                               + " in partition "
                                               + routingStrategy.getPrimaryPartition(key.get())
                                               + "; not " + partition);
                        }
                        count++;
                        keys.add(key);
                    }
                    System.out.println("Node " + node + " contained " + count
                                       + " keys in partition " + partition);
                } catch(Exception e) {
                    System.err.println("Failed to retrieve keys from node [" + node + "]: "
                                       + e.getMessage());
                    errCount++;
                }
            }
            System.out.println("Found " + keys.size() + " keys in partition " + partition);
            for(ByteArray key: keys) {
                keyCount++;
                Map<Node, List<Version>> versions = new HashMap<Node, List<Version>>();
                List<Version> unique = new ArrayList<Version>();
                List<Node> nodes = routingStrategy.routeRequest(key.get());
                for(Node node: nodes) {
                    try {
                        Store<ByteArray, byte[], byte[]> store = nodeStores.get(node.getId());
                        List<Version> result = store.getVersions(key);
                        // System.out.println("Node " + node + " contains " +
                        // result + " for key ["
                        // + new String(key.get()) + "]");
                        versions.put(node, result);
                        unique.addAll(result);
                    } catch(VoldemortException e) {
                        errCount++;
                        System.err.println("Failed to retrieve versions for key ["
                                           + new String(key.get()) + "]: " + e.getMessage());
                    }
                }
                unique = VectorClockInconsistencyResolver.getVersions(unique);
                for(Node node: versions.keySet()) {
                    List<Version> result = versions.get(node);
                    if(!result.containsAll(unique)) {
                        if(printReadRepair) {
                            System.out.println("Node " + node + " is missing versions for key ["
                                               + new String(key.get()) + "];  expected: " + unique
                                               + "; returned: " + result);
                        }
                        repairs++;
                    }
                }
                if(unique.size() > 1) {
                    if(printInconsistencies) {
                        System.out.println("Found multiple versions for key "
                                           + new String(key.get()) + "]: " + unique);
                    }
                    inconsistencies++;
                }
            }
        }
        System.out.println("Examined " + keyCount + " keys and found " + inconsistencies
                           + " inconsistencies and " + repairs + " repairs with " + errCount
                           + " errors");
    }
}
