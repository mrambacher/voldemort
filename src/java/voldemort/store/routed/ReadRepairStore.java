/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright (c) 2010 Nokia Corporation. All rights reserved.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class ReadRepairStore<N> extends DistributingStore<N> {

    private static final Logger logger = LogManager.getLogger(ReadRepairStore.class);
    private final boolean repairReads;
    private final ReadRepairer<N, ByteArray, byte[]> readRepairer;

    public ReadRepairStore(String name,
                           Map<N, Store<ByteArray, byte[]>> stores,
                           StoreDefinition storeDef,
                           int numThreads,
                           long timeout,
                           TimeUnit units,
                           boolean repairReads) {
        this(name,
             stores,
             storeDef,
             Executors.newFixedThreadPool(numThreads),
             timeout,
             units,
             SystemTime.INSTANCE,
             repairReads);
    }

    public ReadRepairStore(String name,
                           Map<N, Store<ByteArray, byte[]>> stores,
                           StoreDefinition storeDef,
                           ExecutorService threadPool,
                           long timeout,
                           TimeUnit units,
                           Time time,
                           boolean repairReads) {
        super(name, stores, storeDef, threadPool, timeout, units, time);
        this.repairReads = repairReads;
        this.readRepairer = new ReadRepairer<N, ByteArray, byte[]>();
    }

    @Override
    public List<Versioned<byte[]>> get(final ByteArray key,
                                       Collection<N> nodes,
                                       int preferred,
                                       int required,
                                       long timeout,
                                       TimeUnit units) throws VoldemortException {
        ParallelTask<N, List<Versioned<byte[]>>> job = getJob(key, nodes);
        Map<N, List<Versioned<byte[]>>> results = job.get(preferred, required, timeout, units);
        List<NodeValue<N, ByteArray, byte[]>> repairs = buildRepairReads(key, results);
        repairReads(repairs);
        return buildResults(results);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Map<N, List<ByteArray>> keys,
                                                          int preferred,
                                                          int required,
                                                          long timeout,
                                                          TimeUnit units) throws VoldemortException {
        ParallelTask<N, Map<ByteArray, List<Versioned<byte[]>>>> job = getAllJob(keys);
        Map<N, Map<ByteArray, List<Versioned<byte[]>>>> results = job.get(preferred,
                                                                          required,
                                                                          timeout,
                                                                          units);
        List<NodeValue<N, ByteArray, byte[]>> repairs = buildRepairReads(keys, results);
        repairReads(repairs);
        return buildResults(results);
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case READ_REPAIRER:
                return this.readRepairer;
            default:
                return super.getCapability(capability);
        }
    }

    /**
     * Returns a null valued NodeValue.
     * 
     * @param node The node.
     * @param key the key.
     * @return NodeValue with null versioned values.
     */
    private NodeValue<N, ByteArray, byte[]> nullValue(N node, ByteArray key) {
        return new NodeValue<N, ByteArray, byte[]>(node, key, new Versioned<byte[]>(null));
    }

    /**
     * Fills nodeValues using provided parameters. In case fetch list is null,
     * it puts in a null entry so that read repair is triggered
     * 
     * @param nodeValues The list which is populated.
     * @param key The key
     * @param node The node where the key was obtained from.
     * @param fetched The versions obtained from the node.
     */
    private void fillRepairReadsValues(final List<NodeValue<N, ByteArray, byte[]>> nodeValues,
                                       final ByteArray key,
                                       N node,
                                       List<Versioned<byte[]>> fetched) {
        if(repairReads) {
            // fetched can be null when used from getAll
            if(fetched == null || fetched.size() == 0) {
                nodeValues.add(nullValue(node, key));
            } else {
                for(Versioned<byte[]> f: fetched)
                    nodeValues.add(new NodeValue<N, ByteArray, byte[]>(node, key, f));
            }
        }
    }

    private List<NodeValue<N, ByteArray, byte[]>> buildRepairReads(ByteArray key,
                                                                   Map<N, List<Versioned<byte[]>>> nodeResults) {
        if(logger.isDebugEnabled()) {
            logger.debug("Read repair for key: " + key.toString());
        }

        List<NodeValue<N, ByteArray, byte[]>> nodeValues = Lists.newArrayListWithExpectedSize(nodeResults.size());
        for(Entry<N, List<Versioned<byte[]>>> entry: nodeResults.entrySet()) {
            final N node = entry.getKey();
            fillRepairReadsValues(nodeValues, key, node, entry.getValue());
        }
        return nodeValues;

    }

    private List<NodeValue<N, ByteArray, byte[]>> buildRepairReads(Map<N, List<ByteArray>> keys,
                                                                   Map<N, Map<ByteArray, List<Versioned<byte[]>>>> values) {
        List<NodeValue<N, ByteArray, byte[]>> nodeValues = Lists.newArrayListWithExpectedSize(values.size());

        for(Entry<N, List<ByteArray>> entry: keys.entrySet()) {
            N node = entry.getKey();
            Map<ByteArray, List<Versioned<byte[]>>> result = values.get(node);
            if(result != null) {
                for(ByteArray key: entry.getValue()) {
                    fillRepairReadsValues(nodeValues, key, node, result.get(key));
                }
            }
        }
        return nodeValues;
    }

    /**
     * Conducts read repairs if needed. The read repairs are conducted by a
     * non-blocking thread.
     * 
     * @param nodeValues The list of node-value mappings as received in get() or
     *        getAll()
     */
    private void repairReads(final List<NodeValue<N, ByteArray, byte[]>> nodeValues) {
        if(!repairReads)
            return;

        final List<NodeValue<N, ByteArray, byte[]>> repairList = readRepairer.getRepairs(nodeValues);

        if(repairList.size() > 0) {

            this.executor.execute(new Runnable() {

                public void run() {
                    for(NodeValue<N, ByteArray, byte[]> v: repairList) {
                        try {
                            if(logger.isDebugEnabled())
                                logger.debug("Doing read repair on node " + v.getNode()
                                             + " for key '" + v.getKey() + "' with version "
                                             + v.getVersion() + ".");
                            Store<ByteArray, byte[]> store = getNodeStore(v.getNode());
                            store.put(v.getKey(), v.getVersioned());
                        } catch(ObsoleteVersionException e) {
                            if(logger.isDebugEnabled())
                                logger.debug("Read repair cancelled due to obsolete version on node "
                                             + v.getNode()
                                             + " for key '"
                                             + v.getKey()
                                             + "' with version "
                                             + v.getVersion()
                                             + ": "
                                             + e.getMessage());
                        } catch(Exception e) {
                            logger.debug("Read repair failed: ", e);
                        }
                    }
                }
            });
        }
    }
}
