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

package voldemort.store.routed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.store.routed.action.ConfigureNodes;
import voldemort.store.routed.action.GetAllConfigureNodes;
import voldemort.store.routed.action.IncrementClock;
import voldemort.store.routed.action.PerformParallelDeleteRequests;
import voldemort.store.routed.action.PerformParallelGetAllRequests;
import voldemort.store.routed.action.PerformParallelGetRequests;
import voldemort.store.routed.action.PerformParallelGetVersionsRequest;
import voldemort.store.routed.action.PerformParallelPutRequests;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * 
 */
public class PipelineRoutedStore extends RoutedStore {

    private Zone clientZone;
    private boolean zoneRoutingEnabled;

    private final FailureDetector failureDetector;

    /**
     * Create a PipelineRoutedStore
     * 
     * @param name The name of the store
     * @param innerStores The mapping of node to client
     * @param nonblockingStores
     * @param slopStores The stores for hints
     * @param cluster Cluster definition
     * @param storeDef Store definition
     * @param repairReads Is read repair enabled?
     * @param clientZoneId Zone the client is in
     * @param timeoutMs Routing timeout
     * @param failureDetector Failure detector object
     */
    public PipelineRoutedStore(String name,
                               DistributedStore<Node, ByteArray, byte[], byte[]> distributor,
                               Cluster cluster,
                               StoreDefinition storeDef,
                               int clientZoneId,
                               long timeoutMs,
                               FailureDetector failureDetector) {
        super(name, distributor, cluster, storeDef, timeoutMs, SystemTime.INSTANCE);

        this.failureDetector = failureDetector;
        this.clientZone = cluster.getZoneById(clientZoneId);
        if(storeDef.getRoutingStrategyType().compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
            zoneRoutingEnabled = true;
        } else {
            zoneRoutingEnabled = false;
        }
    }

    public List<Versioned<byte[]>> get(final ByteArray key, final byte[] transforms) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<List<Versioned<byte[]>>> pipelineData = new BasicPipelineData<List<Versioned<byte[]>>>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);

        final Pipeline pipeline = new Pipeline(Operation.GET, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>>(pipelineData,
                                                                                                                        Event.CONFIGURED,
                                                                                                                        failureDetector,
                                                                                                                        storeDef.getRequiredReads(),
                                                                                                                        routingStrategy,
                                                                                                                        key,
                                                                                                                        clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelGetRequests(pipelineData,
                                                               Event.COMPLETED,
                                                               key,
                                                               transforms,
                                                               storeDef.getPreferredReads(),
                                                               storeDef.getRequiredReads(),
                                                               timeoutMs,
                                                               distributor));

        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>();

        for(Response<ByteArray, List<Versioned<byte[]>>> response: pipelineData.getResponses()) {
            List<Versioned<byte[]>> value = response.getValue();

            if(value != null)
                results.addAll(value);
        }

        return results;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        GetAllPipelineData pipelineData = new GetAllPipelineData();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        Pipeline pipeline = new Pipeline(Operation.GET_ALL, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new GetAllConfigureNodes(pipelineData,
                                                         Event.CONFIGURED,
                                                         failureDetector,
                                                         storeDef.getPreferredReads(),
                                                         storeDef.getRequiredReads(),
                                                         routingStrategy,
                                                         keys,
                                                         transforms,
                                                         clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelGetAllRequests(pipelineData,
                                                                  Event.COMPLETED,
                                                                  storeDef.getPreferredReads(),
                                                                  storeDef.getRequiredReads(),
                                                                  timeoutMs,
                                                                  distributor));

        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        return pipelineData.getResult();
    }

    public List<Version> getVersions(final ByteArray key) {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<List<Version>> pipelineData = new BasicPipelineData<List<Version>>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountReads());
        else
            pipelineData.setZonesRequired(null);
        Pipeline pipeline = new Pipeline(Operation.GET_VERSIONS, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<List<Version>, BasicPipelineData<List<Version>>>(pipelineData,
                                                                                                    Event.CONFIGURED,
                                                                                                    failureDetector,
                                                                                                    storeDef.getRequiredReads(),
                                                                                                    routingStrategy,
                                                                                                    key,
                                                                                                    clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelGetVersionsRequest(pipelineData,
                                                                      Event.COMPLETED,
                                                                      key,
                                                                      storeDef.getPreferredReads(),
                                                                      storeDef.getRequiredReads(),
                                                                      timeoutMs,
                                                                      distributor));

        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        List<Version> results = new ArrayList<Version>();

        for(Response<ByteArray, List<Version>> response: pipelineData.getResponses())
            results.addAll(response.getValue());

        return results;
    }

    public boolean delete(final ByteArray key, final Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        BasicPipelineData<Boolean> pipelineData = new BasicPipelineData<Boolean>();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountWrites());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStoreName(name);
        Pipeline pipeline = new Pipeline(Operation.DELETE, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<Boolean, BasicPipelineData<Boolean>>(pipelineData,
                                                                                        Event.CONFIGURED,
                                                                                        failureDetector,
                                                                                        storeDef.getRequiredWrites(),
                                                                                        routingStrategy,
                                                                                        key,
                                                                                        clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformParallelDeleteRequests(pipelineData,
                                                                  Event.COMPLETED,
                                                                  key,
                                                                  version,
                                                                  storeDef.getPreferredWrites(),
                                                                  storeDef.getRequiredWrites(),
                                                                  timeoutMs,
                                                                  distributor));

        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();
        for(Response<ByteArray, Boolean> response: pipelineData.getResponses()) {
            if(response.getValue().booleanValue())
                return true;
        }

        return false;
    }

    public Version put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PutPipelineData pipelineData = new PutPipelineData();
        if(zoneRoutingEnabled)
            pipelineData.setZonesRequired(storeDef.getZoneCountWrites());
        else
            pipelineData.setZonesRequired(null);
        pipelineData.setStartTimeNs(System.nanoTime());
        pipelineData.setStoreName(name);

        Pipeline pipeline = new Pipeline(Operation.PUT, timeoutMs, TimeUnit.MILLISECONDS);

        pipeline.addEventAction(Event.STARTED,
                                new ConfigureNodes<Version, PutPipelineData>(pipelineData,
                                                                             Event.CONFIGURED,
                                                                             failureDetector,
                                                                             storeDef.getRequiredWrites(),
                                                                             routingStrategy,
                                                                             key,
                                                                             clientZone));
        pipeline.addEventAction(Event.CONFIGURED,
                                new PerformSerialPutRequests(pipelineData,
                                                             Event.RESPONSES_RECEIVED,
                                                             key,
                                                             versioned,
                                                             transforms,
                                                             timeoutMs,
                                                             distributor,
                                                             storeDef.getRequiredWrites(),
                                                             time,
                                                             Event.MASTER_DETERMINED));
        pipeline.addEventAction(Event.MASTER_DETERMINED,
                                new PerformParallelPutRequests(pipelineData,
                                                               Event.RESPONSES_RECEIVED,
                                                               key,
                                                               transforms,
                                                               storeDef.getPreferredWrites(),
                                                               storeDef.getRequiredWrites(),
                                                               timeoutMs,
                                                               distributor));
        pipeline.addEventAction(Event.RESPONSES_RECEIVED, new IncrementClock(pipelineData,
                                                                             Event.COMPLETED,
                                                                             versioned,
                                                                             time));
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();
        return pipelineData.getVersionedCopy().getVersion();
    }

    @Override
    public void close() {

        distributor.close();
        super.close();
    }

}
