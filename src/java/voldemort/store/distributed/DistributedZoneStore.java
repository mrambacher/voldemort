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
package voldemort.store.distributed;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.StoreDefinition;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * A Distributed Store that is zone-aware.
 */
public class DistributedZoneStore<K, V, T> extends DelegatingDistributedStore<Node, K, V, T> {

    private final StoreDefinition storeDef;
    private final Map<Node, Integer> nodesByZone;
    protected final ResultsBuilder<Node, List<Versioned<V>>> getBuilder;
    protected final ResultsBuilder<Node, List<Version>> versionsBuilder;
    protected final ResultsBuilder<Node, Version> putBuilder;
    protected final ResultsBuilder<Node, Boolean> deleteBuilder;
    private final Zone clientZone;

    public static <K, V, T> DistributedZoneStore<K, V, T> create(DistributedStore<Node, K, V, T> distributor,
                                                                 StoreDefinition storeDef,
                                                                 Cluster cluster,
                                                                 int zoneId,
                                                                 boolean unique) {
        return new DistributedZoneStore<K, V, T>(distributor, storeDef, cluster, zoneId, unique);
    }

    public DistributedZoneStore(DistributedStore<Node, K, V, T> distributor,
                                StoreDefinition storeDef,
                                Cluster cluster,
                                int zoneId,
                                boolean unique) {

        super(distributor);
        this.clientZone = cluster.getZoneById(zoneId);
        this.storeDef = storeDef;
        this.getBuilder = DistributedStoreFactory.GetBuilder(unique);
        this.versionsBuilder = DistributedStoreFactory.GetVersionsBuilder(unique);
        this.putBuilder = DistributedStoreFactory.PutBuilder();
        this.deleteBuilder = DistributedStoreFactory.DeleteBuilder();
        Map<Integer, Collection<Node>> zonesByNode = cluster.getZonesByNode();
        this.nodesByZone = Maps.newHashMap();
        for(Map.Entry<Integer, Collection<Node>> entry: zonesByNode.entrySet()) {
            for(Node node: entry.getValue()) {
                nodesByZone.put(node, entry.getKey());
            }
        }
    }

    private Map<Integer, Collection<Node>> getZonesForNodes(Collection<Node> nodes) {
        Multimap<Integer, Node> zonesByNode = HashMultimap.create();
        for(Node node: nodes) {
            int zone = nodesByZone.get(node);
            zonesByNode.put(zone, node);
        }
        return zonesByNode.asMap();
    }

    @Override
    public DistributedFuture<Node, List<Versioned<V>>> submitGet(final K key,
                                                                 final T transform,
                                                                 Collection<Node> nodes,
                                                                 int preferred,
                                                                 int required)
            throws VoldemortException {
        DistributedStoreFactory.checkRequiredReads(nodes.size(), required);
        Map<Integer, Collection<Node>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountReads());
        Collection<DistributedFuture<Node, List<Versioned<V>>>> futures = Lists.newArrayList();

        for(Collection<Node> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<Node, List<Versioned<V>>> future = super.submitGet(key,
                                                                                 transform,
                                                                                 nodes,
                                                                                 preferredZone,
                                                                                 1);
            futures.add(future);
        }
        return new ZoneFutureTask<Node, List<Versioned<V>>>(VoldemortOpCode.GET.getOperationName(),
                                                            futures,
                                                            this.getBuilder,
                                                            storeDef.getZoneCountReads(),
                                                            storeDef.getZoneCountReads());
    }

    @Override
    public DistributedFuture<Node, List<Version>> submitGetVersions(final K key,
                                                                    Collection<Node> nodes,
                                                                    int preferred,
                                                                    int required)
            throws VoldemortException {
        DistributedStoreFactory.checkRequiredReads(nodes.size(), required);
        Map<Integer, Collection<Node>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountReads());
        Collection<DistributedFuture<Node, List<Version>>> futures = Lists.newArrayList();

        for(Collection<Node> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<Node, List<Version>> future = super.submitGetVersions(key,
                                                                                    nodes,
                                                                                    preferredZone,
                                                                                    1);
            futures.add(future);
        }
        return new ZoneFutureTask<Node, List<Version>>(VoldemortOpCode.GET_VERSION.getOperationName(),
                                                       futures,
                                                       this.versionsBuilder,
                                                       storeDef.getZoneCountReads(),
                                                       storeDef.getZoneCountReads());
    }

    @Override
    public DistributedFuture<Node, Boolean> submitDelete(K key,
                                                         Version version,
                                                         Collection<Node> nodes,
                                                         int preferred,
                                                         int required) throws VoldemortException {
        DistributedStoreFactory.checkRequiredWrites(nodes.size(), required);
        Map<Integer, Collection<Node>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountWrites());
        Collection<DistributedFuture<Node, Boolean>> futures = Lists.newArrayList();

        for(Collection<Node> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<Node, Boolean> future = super.submitDelete(key,
                                                                         version,
                                                                         nodes,
                                                                         preferredZone,
                                                                         1);
            futures.add(future);
        }
        return new ZoneFutureTask<Node, Boolean>(VoldemortOpCode.DELETE.getOperationName(),
                                                 futures,
                                                 deleteBuilder,
                                                 storeDef.getZoneCountWrites(),
                                                 storeDef.getZoneCountWrites());
    }

    @Override
    public DistributedFuture<Node, Version> submitPut(K key,
                                                      Versioned<V> value,
                                                      T transform,
                                                      Collection<Node> nodes,
                                                      int preferred,
                                                      int required) throws VoldemortException {
        DistributedStoreFactory.checkRequiredWrites(nodes.size(), required);
        Map<Integer, Collection<Node>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountWrites());
        Collection<DistributedFuture<Node, Version>> futures = Lists.newArrayList();

        for(Collection<Node> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<Node, Version> future = super.submitPut(key,
                                                                      value,
                                                                      transform,
                                                                      nodes,
                                                                      preferredZone,
                                                                      1);
            futures.add(future);
        }
        return new ZoneFutureTask<Node, Version>(VoldemortOpCode.PUT.getOperationName(),
                                                 futures,
                                                 putBuilder,
                                                 storeDef.getZoneCountWrites(),
                                                 storeDef.getZoneCountWrites());
    }
}
