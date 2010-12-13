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

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;

public class PerformParallelDeleteRequests extends
        AbstractKeyBasedAction<ByteArray, Boolean, BasicPipelineData<Boolean>> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final Version version;
    private final DistributedStore<Node, ByteArray, byte[], byte[]> distributor;

    public PerformParallelDeleteRequests(BasicPipelineData<Boolean> pipelineData,
                                         Event completeEvent,
                                         ByteArray key,
                                         Version version,
                                         int preferred,
                                         int required,
                                         long timeoutMs,
                                         DistributedStore<Node, ByteArray, byte[], byte[]> distributor) {
        super(pipelineData, completeEvent, key);
        this.version = version;
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.distributor = distributor;
    }

    public void execute(final Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();
        int attempts = Math.min(preferred, nodes.size());

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        try {
            DistributedFuture<Node, Boolean> future = distributor.submitDelete(key,
                                                                               version,
                                                                               nodes,
                                                                               attempts,
                                                                               this.required);
            boolean deleted = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            Map<Node, Boolean> results = future.getResults();
            for(Map.Entry<Node, Boolean> result: results.entrySet()) {
                Node node = result.getKey();
                pipelineData.getResponses()
                            .add(new Response<ByteArray, Boolean>(node,
                                                                  key,
                                                                  result.getValue(),
                                                                  future.getDuration(node,
                                                                                     TimeUnit.MILLISECONDS)));
                pipelineData.getZoneResponses().add(node.getZoneId());
                pipelineData.incrementSuccesses();
            }
            pipeline.addEvent(completeEvent);
        } catch(VoldemortException ex) {
            pipelineData.setFatalError(ex);
            pipeline.addEvent(Event.ERROR);
        }
    }
}
