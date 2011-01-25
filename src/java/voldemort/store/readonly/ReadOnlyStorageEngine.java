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

package voldemort.store.readonly;

import java.io.File;
import java.util.Collection;

import voldemort.client.protocol.VoldemortFilter;
import voldemort.routing.RoutingStrategy;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * A read-only store that fronts a big file
 * 
 * 
 */
public class ReadOnlyStorageEngine extends ReadOnlyStore implements
        StorageEngine<ByteArray, byte[], byte[]> {

    private final StoreDefinition storeDef;

    /**
     * Create an instance of the store
     * 
     * @param name The name of the store
     * @param searchStrategy The algorithm to use for searching for keys
     * @param storeDir The directory in which the .data and .index files reside
     * @param numBackups The number of backups of these files to retain
     */
    public ReadOnlyStorageEngine(StoreDefinition storeDef,
                                 SearchStrategy searchStrategy,
                                 RoutingStrategy routingStrategy,
                                 int nodeId,
                                 File storeDir,
                                 int numBackups) {
        super(storeDef.getName(), searchStrategy, routingStrategy, nodeId, storeDir, numBackups);
        this.storeDef = storeDef;
    }

    public StoreDefinition getStoreDefinition() {
        return storeDef;
    }

    public ClosableIterator<ByteArray> keys(Collection<Integer> partitions,
                                            VoldemortFilter<ByteArray, byte[]> filter) {
        throw new UnsupportedOperationException("Iteration is not supported for "
                                                + getClass().getName());
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(Collection<Integer> partitions,
                                                                        VoldemortFilter<ByteArray, byte[]> filter,
                                                                        byte[] transforms) {
        throw new UnsupportedOperationException("Iteration is not supported for "
                                                + getClass().getName());
    }

    /**
     * Deletes all of the keys that match the specified filter
     * 
     * @param filter The filter to compare values to. All matching values are
     *        removed
     */
    public void deleteEntries(VoldemortFilter<ByteArray, byte[]> filter) {
        throw new UnsupportedOperationException("DeleteEntries is not supported for "
                                                + getClass().getName());

    }

    public void deletePartitions(Collection<Integer> partitions) {
        throw new UnsupportedOperationException("Delete partitions is not supported for "
                                                + getClass().getName());

    }

    public void truncate() {
        if(isOpen)
            close();
        Utils.rm(storeDir);
        logger.debug("Truncate successful for read-only store ");
    }
}
