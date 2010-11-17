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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class PerformParallelPutRequests extends
        AbstractKeyBasedAction<ByteArray, Version, PutPipelineData> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final DistributedStore<Node, ByteArray, byte[]> distributor;

    public PerformParallelPutRequests(PutPipelineData pipelineData,
                                      Event completeEvent,
                                      ByteArray key,
                                      int preferred,
                                      int required,
                                      long timeoutMs,
                                      DistributedStore<Node, ByteArray, byte[]> distributor) {
        super(pipelineData, completeEvent, key);
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.distributor = distributor;
    }

    public void execute(final Pipeline pipeline) {
        Node master = pipelineData.getMaster();
        Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();

        if(logger.isDebugEnabled())
            logger.debug("Serial put requests determined master node as " + master.getId()
                         + ", submitting remaining requests in parallel");

        List<Node> nodes = pipelineData.getNodes();
        int firstParallelNodeIndex = nodes.indexOf(master) + 1;
        int attempts = nodes.size() - firstParallelNodeIndex;
        int blocks = Math.min(required - 1, attempts);
        List<Node> available = new ArrayList<Node>(attempts);
        while(firstParallelNodeIndex < nodes.size()) {
            available.add(nodes.get(firstParallelNodeIndex++));
        }

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        DistributedFuture<Node, Version> future = distributor.submitPut(key,
                                                                        versionedCopy,
                                                                        available,
                                                                        attempts,
                                                                        blocks);

        boolean quorumSatisfied = true;
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch(InsufficientSuccessfulNodesException e) {
            pipeline.addEvent(Event.ERROR);
            quorumSatisfied = false;
        }
        Set<Node> successes = new HashSet<Node>(future.getResults().keySet());
        for(Node successfulNodes: successes) {
            pipelineData.incrementSuccesses();
            pipelineData.getZoneResponses().add(successfulNodes.getZoneId());
        }

        if(quorumSatisfied) {
            if(pipelineData.getZonesRequired() != null) {
                int zonesSatisfied = pipelineData.getZoneResponses().size();
                if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1)) {
                    pipeline.addEvent(completeEvent);
                } else {
                    long timeMs = (System.nanoTime() - pipelineData.getStartTimeNs())
                                  / Time.NS_PER_MS;

                    if((timeoutMs - timeMs) > 0) {
                        try {
                            Map<Node, Version> responses = future.complete(timeoutMs - timeMs,
                                                                           TimeUnit.MILLISECONDS);
                            for(Node node: responses.keySet()) {
                                if(!successes.contains(node)) {
                                    pipelineData.incrementSuccesses();
                                    pipelineData.getZoneResponses().add(node.getZoneId());
                                    successes.add(node);
                                }
                            }
                            if(pipelineData.getZoneResponses().size() >= (pipelineData.getZonesRequired() + 1)) {
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
                        } catch(VoldemortException e) {
                            if(logger.isEnabledFor(Level.WARN))
                                logger.warn(e, e);
                            pipelineData.setFatalError(e);
                            pipeline.addEvent(Event.ERROR);
                        }
                    }
                }
            } else {
                pipeline.addEvent(completeEvent);
            }
        }
    }
}
