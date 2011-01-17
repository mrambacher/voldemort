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

import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.distributed.DistributedStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * A Store which multiplexes requests to different internal Stores
 * 
 * 
 */
public abstract class RoutedStore implements Store<ByteArray, byte[], byte[]> {

    protected final String name;
    protected final DistributedStore<Node, ByteArray, byte[], byte[]> distributor;
    protected final long timeoutMs;
    protected final Time time;
    protected final StoreDefinition storeDef;
    protected volatile RoutingStrategy routingStrategy;
    protected final Logger logger = Logger.getLogger(getClass());

    protected RoutedStore(String name,
                          DistributedStore<Node, ByteArray, byte[], byte[]> distributor,
                          Cluster cluster,
                          StoreDefinition storeDef,
                          long timeoutMs,
                          Time time) {
        this.name = name;
        this.distributor = distributor;
        this.timeoutMs = timeoutMs;
        this.time = Utils.notNull(time);
        this.storeDef = storeDef;
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
    }

    public void updateRoutingStrategy(RoutingStrategy routingStrategy) {
        logger.info("Updating routing strategy for RoutedStore:" + getName());
        this.routingStrategy = routingStrategy;
    }

    public String getName() {
        return this.name;
    }

    public void close() {
        distributor.close();
    }

    protected void checkRequiredReads(final int numNodes)
            throws InsufficientOperationalNodesException {
        int requiredReads = storeDef.getRequiredReads();
        if(numNodes < requiredReads) {
            if(logger.isDebugEnabled()) {
                logger.debug("Quorom exception - not enough nodes required: " + requiredReads
                             + ", found: " + numNodes);
            }

            throw new InsufficientOperationalNodesException("Only "
                                                                    + numNodes
                                                                    + " nodes in preference list, but "
                                                                    + requiredReads
                                                                    + " reads required.",
                                                            numNodes,
                                                            requiredReads);
        }
    }

    protected void checkRequiredWrites(final int numNodes)
            throws InsufficientOperationalNodesException {
        int requiredWrites = storeDef.getRequiredWrites();
        if(numNodes < requiredWrites) {
            if(logger.isDebugEnabled()) {
                logger.debug("Quorom exception - not enough nodes required: " + requiredWrites
                             + ", found: " + numNodes);
            }

            throw new InsufficientOperationalNodesException("Only "
                                                                    + numNodes
                                                                    + " nodes in preference list, but "
                                                                    + requiredWrites
                                                                    + " writes required.",
                                                            numNodes,
                                                            requiredWrites);
        }
    }

    public Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> getNodeStores() {
        return this.distributor.getNodeStores();
    }

    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case ROUTING_STRATEGY:
                return this.routingStrategy;
            case VERSION_INCREMENTING:
                return true;
            default:
                return distributor.getCapability(capability);
        }
    }

}
