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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RouteToAllStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.utils.ByteArray;

public class GetAllConfigureNodesTest extends AbstractActionTest {

    @Test
    public void testConfigureNodes() throws Exception {
        int preferred = cluster.getNumberOfNodes() - 1;
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(),
                                                                        preferred);
        GetAllPipelineData pipelineData = new GetAllPipelineData();
        List<ByteArray> keys = new ArrayList<ByteArray>();

        for(int i = 0; i < 10; i++)
            keys.add(TestUtils.toByteArray("key-" + i));

        GetAllConfigureNodes action = new GetAllConfigureNodes(pipelineData,
                                                               Event.COMPLETED,
                                                               failureDetector,
                                                               preferred,
                                                               preferred - 1,
                                                               routingStrategy,
                                                               keys,
                                                               null);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED, action);
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();

        if(pipelineData.getFatalError() != null)
            throw pipelineData.getFatalError();

        Map<Node, List<ByteArray>> pipelineNodes = pipelineData.getNodeToKeysMap();
        for(ByteArray key: keys) {
            List<Node> preferredNodes = routingStrategy.routeRequest(key.get());
            assertEquals(preferred, preferredNodes.size());
            for(Node node: preferredNodes) {
                assertTrue("Pipeline node contains key", pipelineNodes.get(node).contains(key));
            }
        }
    }

    public void testConfigureNodesNotEnoughNodes() throws Exception {
        for(Node node: cluster.getNodes())
            failureDetector.recordException(node,
                                            0,
                                            new UnreachableStoreException("Test for "
                                                                          + getClass().getName()));

        RoutingStrategy routingStrategy = new RouteToAllStrategy(cluster.getNodes());
        GetAllPipelineData pipelineData = new GetAllPipelineData();

        GetAllConfigureNodes action = new GetAllConfigureNodes(pipelineData,
                                                               Event.COMPLETED,
                                                               failureDetector,
                                                               1,
                                                               1,
                                                               routingStrategy,
                                                               Arrays.asList(aKey),
                                                               null);

        Pipeline pipeline = new Pipeline(Operation.GET, 10000, TimeUnit.MILLISECONDS);
        pipeline.addEventAction(Event.STARTED, action);
        pipeline.addEvent(Event.STARTED);
        pipeline.execute();
        assertEquals("Caught Exception",
                     InsufficientOperationalNodesException.class,
                     pipelineData.getFatalError().getClass());
    }

}
