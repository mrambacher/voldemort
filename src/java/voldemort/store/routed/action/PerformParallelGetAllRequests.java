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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class PerformParallelGetAllRequests
        extends
        AbstractAction<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final DistributedStore<Node, ByteArray, byte[]> distributor;

    public PerformParallelGetAllRequests(GetAllPipelineData pipelineData,
                                         Event completeEvent,
                                         int preferred,
                                         int required,
                                         long timeoutMs,
                                         DistributedStore<Node, ByteArray, byte[]> distributor) {
        super(pipelineData, completeEvent);
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.distributor = distributor;
    }

    public void execute(final Pipeline pipeline) {
        Map<Node, List<ByteArray>> nodesToKeys = pipelineData.getNodeToKeysMap();
        int attempts = nodesToKeys.size();

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        DistributedFuture<Node, Map<ByteArray, List<Versioned<byte[]>>>> future = distributor.submitGetAll(nodesToKeys,
                                                                                                           attempts,
                                                                                                           required);

        try {
            Map<ByteArray, List<Versioned<byte[]>>> results = future.get(timeoutMs * 3,
                                                                         TimeUnit.MILLISECONDS);
            pipelineData.getResult().putAll(results);
        } catch(VoldemortApplicationException e) {
            pipelineData.setFatalError(e);
            pipeline.addEvent(Event.ERROR);
            return;
        }
        pipeline.addEvent(completeEvent);
    }
}