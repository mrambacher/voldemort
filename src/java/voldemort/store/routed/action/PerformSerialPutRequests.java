/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed.action;

import java.util.List;
import java.util.concurrent.TimeUnit;

import voldemort.cluster.Node;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

public class PerformSerialPutRequests extends
        AbstractKeyBasedAction<ByteArray, Version, PutPipelineData> {

    private final int required;

    private final long timeoutMs;

    private final DistributedStore<Node, ByteArray, byte[]> distributor;

    private final Versioned<byte[]> versioned;

    private final Time time;

    private final Event masterDeterminedEvent;

    public PerformSerialPutRequests(PutPipelineData pipelineData,
                                    Event completeEvent,
                                    ByteArray key,
                                    Versioned<byte[]> versioned,
                                    long timeoutMs,
                                    DistributedStore<Node, ByteArray, byte[]> distributor,
                                    int required,
                                    Time time,
                                    Event masterDeterminedEvent) {
        super(pipelineData, completeEvent, key);
        this.distributor = distributor;
        this.timeoutMs = timeoutMs;
        this.required = required;
        this.versioned = versioned;
        this.time = time;
        this.masterDeterminedEvent = masterDeterminedEvent;
    }

    private Versioned<byte[]> incremented(Versioned<byte[]> versioned, int nodeId) {
        Version incremented = VersionFactory.cloneVersion(versioned.getVersion());
        incremented.incrementClock(nodeId, time.getMilliseconds());

        return new Versioned<byte[]>(versioned.getValue(), incremented, versioned.getMetadata());
    }

    protected void checkRequiredWrites(final int numNodes)
            throws InsufficientOperationalNodesException {
        if(numNodes < required) {
            if(logger.isDebugEnabled()) {
                logger.debug("Quorom exception - not enough nodes required: " + required
                             + ", found: " + numNodes);
            }

            throw new InsufficientOperationalNodesException("Only "
                                                                    + numNodes
                                                                    + " nodes in preference list, but "
                                                                    + required
                                                                    + " writes required.",
                                                            numNodes,
                                                            required);
        }
    }

    public void execute(Pipeline pipeline) {
        int currentNode = 0;
        List<Node> nodes = pipelineData.getNodes();

        if(logger.isDebugEnabled())
            logger.debug("Performing serial put requests to determine master");

        for(; currentNode < nodes.size(); currentNode++) {
            Node node = nodes.get(currentNode);
            checkRequiredWrites(nodes.size() - currentNode);
            pipelineData.incrementNodeIndex();

            final Versioned<byte[]> versionedCopy = incremented(versioned, node.getId());

            if(logger.isTraceEnabled())
                logger.trace("Attempt #" + (currentNode + 1) + " to perform put (node "
                             + node.getId() + ")");

            try {
                AsynchronousStore<ByteArray, byte[]> async = distributor.getNodeStore(node);
                StoreFuture<Version> future = async.submitPut(key, versionedCopy);
                future.get(this.timeoutMs, TimeUnit.MILLISECONDS);
                pipelineData.incrementSuccesses();

                if(logger.isTraceEnabled())
                    logger.trace("Put on node " + node.getId() + " succeeded, using as master");
                pipelineData.setMaster(node);
                pipelineData.setVersionedCopy(versionedCopy);
                pipelineData.getZoneResponses().add(node.getZoneId());
                break;
            } catch(Exception e) {
                if(handleResponseError(e, node, pipeline))
                    return;
            }
        }

        if(pipelineData.getSuccesses() < 1) {
            List<Exception> failures = pipelineData.getFailures();
            pipelineData.setFatalError(new InsufficientOperationalNodesException("No master node succeeded!",
                                                                                 failures.size() > 0 ? failures.get(0)
                                                                                                    : null,
                                                                                 0,
                                                                                 required));
            pipeline.addEvent(Event.ERROR);
            return;
        }

        currentNode++;

        // There aren't any more requests to make...
        if(currentNode == nodes.size()) {
            if(pipelineData.getSuccesses() < required) {
                pipelineData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                             + " "
                                                                                             + pipeline.getOperation()
                                                                                                       .getSimpleName()
                                                                                             + "s required, but only "
                                                                                             + pipelineData.getSuccesses()
                                                                                             + " succeeded",
                                                                                     pipelineData.getFailures(),
                                                                                     0,
                                                                                     required));
                pipeline.addEvent(Event.ERROR);
            } else {
                if(pipelineData.getZonesRequired() != null) {

                    int zonesSatisfied = pipelineData.getZoneResponses().size();
                    if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                        pipeline.addEvent(completeEvent);
                    } else {
                        pipelineData.setFatalError(new InsufficientZoneResponsesException((pipelineData.getZonesRequired() + 1)
                                                                                          + " "
                                                                                          + pipeline.getOperation()
                                                                                                    .getSimpleName()
                                                                                          + "s required zone, but only "
                                                                                          + zonesSatisfied
                                                                                          + " succeeded"));

                        pipeline.addEvent(Event.ERROR);
                    }

                } else {
                    pipeline.addEvent(completeEvent);
                }
            }
        } else {
            pipeline.addEvent(masterDeterminedEvent);
        }
    }
}
