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

import voldemort.VoldemortApplicationException;
import voldemort.cluster.Node;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PipelineData;
import voldemort.store.routed.ReadRepairer;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public abstract class AbstractReadRepair<K, V, PD extends PipelineData<K, V>> extends
        AbstractAction<K, V, PD> {

    private final int preferred;

    private final DistributedStore<Node, ByteArray, byte[]> distributor;

    private final ReadRepairer<Node, ByteArray, byte[]> readRepairer;

    private final List<NodeValue<Node, ByteArray, byte[]>> nodeValues;

    public AbstractReadRepair(PD pipelineData,
                              Event completeEvent,
                              int preferred,
                              DistributedStore<Node, ByteArray, byte[]> distributor,
                              ReadRepairer<Node, ByteArray, byte[]> readRepairer) {
        super(pipelineData, completeEvent);
        this.preferred = preferred;
        this.distributor = distributor;
        this.readRepairer = readRepairer;
        this.nodeValues = Lists.newArrayListWithExpectedSize(pipelineData.getResponses().size());
    }

    protected abstract void insertNodeValues();

    protected void insertNodeValue(Node node, ByteArray key, List<Versioned<byte[]>> value) {
        if(value.size() == 0) {
            Versioned<byte[]> versioned = new Versioned<byte[]>(null);
            nodeValues.add(new NodeValue<Node, ByteArray, byte[]>(node, key, versioned));
        } else {
            for(Versioned<byte[]> versioned: value)
                nodeValues.add(new NodeValue<Node, ByteArray, byte[]>(node, key, versioned));
        }
    }

    public void execute(Pipeline pipeline) {
        insertNodeValues();

        if(nodeValues.size() > 1 && preferred > 1) {
            List<NodeValue<Node, ByteArray, byte[]>> toReadRepair = Lists.newArrayList();

            /*
             * We clone after computing read repairs in the assumption that the
             * output will be smaller than the input. Note that we clone the
             * version, but not the key or value as the latter two are not
             * mutated.
             */
            for(NodeValue<Node, ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
                Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                              ((VectorClock) v.getVersion()).clone());
                toReadRepair.add(new NodeValue<Node, ByteArray, byte[]>(v.getNode(),
                                                                        v.getKey(),
                                                                        versioned));
            }

            for(NodeValue<Node, ByteArray, byte[]> v: toReadRepair) {
                try {
                    if(logger.isDebugEnabled())
                        logger.debug("Doing read repair on node " + v.getNode() + " for key '"
                                     + v.getKey() + "' with version " + v.getVersion() + ".");
                    AsynchronousStore<ByteArray, byte[]> async = distributor.getNodeStore(v.getNode());
                    async.submitPut(v.getKey(), v.getVersioned());
                } catch(VoldemortApplicationException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Read repair cancelled due to application level exception on node "
                                     + v.getNode()
                                     + " for key '"
                                     + v.getKey()
                                     + "' with version "
                                     + v.getVersion() + ": " + e.getMessage());
                } catch(Exception e) {
                    logger.debug("Read repair failed: ", e);
                }
            }
        }

        pipeline.addEvent(completeEvent);
    }

}
