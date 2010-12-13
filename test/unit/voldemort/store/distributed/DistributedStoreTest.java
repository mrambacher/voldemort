/*
 * Copyright 2008-2009 LinkedIn, Inc
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

import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.utils.ByteArray;

/**
 * Basic tests for RoutedStore
 * 
 * 
 */
public class DistributedStoreTest extends AbstractDistributedStoreTest {

    public DistributedStoreTest() {
        super("test");
    }

    @Override
    protected DistributedStore<Node, ByteArray, byte[], byte[]> buildDistributedStore(Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> stores,
                                                                                      Cluster cluster,
                                                                                      StoreDefinition storeDef,
                                                                                      boolean makeUnique) {
        return new DistributedParallelStore<Node, ByteArray, byte[], byte[]>(stores,
                                                                             storeDef,
                                                                             makeUnique);
    }
}
