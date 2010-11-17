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
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * A Distributed Store that is zone-aware.
 */
public class DistributedZoneStore<N, K, V> extends DelegatingDistributedStore<N, K, V> {

    private final StoreDefinition storeDef;
    private final Map<N, Integer> nodesByZone;
    protected final ResultsBuilder<N, List<Versioned<V>>> getBuilder;
    protected final ResultsBuilder<N, List<Version>> versionsBuilder;
    protected final ResultsBuilder<N, Version> putBuilder;
    protected final ResultsBuilder<N, Boolean> deleteBuilder;

    public DistributedZoneStore(DistributedStore<N, K, V> distributor,
                                StoreDefinition storeDef,
                                Map<Integer, Collection<N>> zonesByNode,
                                boolean unique) {
        super(distributor);
        this.storeDef = storeDef;
        this.getBuilder = DistributedStoreFactory.GetBuilder(unique);
        this.versionsBuilder = DistributedStoreFactory.GetVersionsBuilder(unique);
        this.putBuilder = DistributedStoreFactory.PutBuilder();
        this.deleteBuilder = DistributedStoreFactory.DeleteBuilder();
        this.nodesByZone = Maps.newHashMap();
        for(Map.Entry<Integer, Collection<N>> entry: zonesByNode.entrySet()) {
            for(N node: entry.getValue()) {
                nodesByZone.put(node, entry.getKey());
            }
        }
    }

    private Map<Integer, Collection<N>> getZonesForNodes(Collection<N> nodes) {
        Multimap<Integer, N> zonesByNode = HashMultimap.create();
        for(N node: nodes) {
            int zone = nodesByZone.get(node);
            zonesByNode.put(zone, node);
        }
        return zonesByNode.asMap();
    }

    @Override
    public DistributedFuture<N, List<Versioned<V>>> submitGet(final K key,
                                                              Collection<N> nodes,
                                                              int preferred,
                                                              int required)
            throws VoldemortException {
        DistributedStoreFactory.checkRequiredReads(nodes.size(), required);
        Map<Integer, Collection<N>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountReads());
        Collection<DistributedFuture<N, List<Versioned<V>>>> futures = Lists.newArrayList();

        for(Collection<N> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<N, List<Versioned<V>>> future = super.submitGet(key,
                                                                              nodes,
                                                                              preferredZone,
                                                                              1);
            futures.add(future);
        }
        return new ZoneFutureTask<N, List<Versioned<V>>>(AsynchronousStore.Operations.GET.name(),
                                                         futures,
                                                         this.getBuilder,
                                                         storeDef.getZoneCountReads(),
                                                         storeDef.getZoneCountReads());
    }

    @Override
    public DistributedFuture<N, List<Version>> submitGetVersions(final K key,
                                                                 Collection<N> nodes,
                                                                 int preferred,
                                                                 int required)
            throws VoldemortException {
        DistributedStoreFactory.checkRequiredReads(nodes.size(), required);
        Map<Integer, Collection<N>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountReads());
        Collection<DistributedFuture<N, List<Version>>> futures = Lists.newArrayList();

        for(Collection<N> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<N, List<Version>> future = super.submitGetVersions(key,
                                                                                 nodes,
                                                                                 preferredZone,
                                                                                 1);
            futures.add(future);
        }
        return new ZoneFutureTask<N, List<Version>>(AsynchronousStore.Operations.GETVERSIONS.name(),
                                                    futures,
                                                    this.versionsBuilder,
                                                    storeDef.getZoneCountReads(),
                                                    storeDef.getZoneCountReads());
    }

    @Override
    public DistributedFuture<N, Boolean> submitDelete(K key,
                                                      Version version,
                                                      Collection<N> nodes,
                                                      int preferred,
                                                      int required) throws VoldemortException {
        DistributedStoreFactory.checkRequiredWrites(nodes.size(), required);
        Map<Integer, Collection<N>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountWrites());
        Collection<DistributedFuture<N, Boolean>> futures = Lists.newArrayList();

        for(Collection<N> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<N, Boolean> future = super.submitDelete(key,
                                                                      version,
                                                                      nodes,
                                                                      preferredZone,
                                                                      1);
            futures.add(future);
        }
        return new ZoneFutureTask<N, Boolean>(AsynchronousStore.Operations.DELETE.name(),
                                              futures,
                                              deleteBuilder,
                                              storeDef.getZoneCountWrites(),
                                              storeDef.getZoneCountWrites());
    }

    @Override
    public DistributedFuture<N, Version> submitPut(K key,
                                                   Versioned<V> value,
                                                   Collection<N> nodes,
                                                   int preferred,
                                                   int required) throws VoldemortException {
        DistributedStoreFactory.checkRequiredWrites(nodes.size(), required);
        Map<Integer, Collection<N>> zones = getZonesForNodes(nodes);
        DistributedStoreFactory.checkRequiredZones(zones.keySet().size(),
                                                   storeDef.getZoneCountWrites());
        Collection<DistributedFuture<N, Version>> futures = Lists.newArrayList();

        for(Collection<N> zoneNodes: zones.values()) {
            int preferredZone = Math.min(preferred, zoneNodes.size());
            DistributedFuture<N, Version> future = super.submitPut(key,
                                                                   value,
                                                                   nodes,
                                                                   preferredZone,
                                                                   1);
            futures.add(future);
        }
        return new ZoneFutureTask<N, Version>(AsynchronousStore.Operations.PUT.name(),
                                              futures,
                                              putBuilder,
                                              storeDef.getZoneCountWrites(),
                                              storeDef.getZoneCountWrites());
    }
}
