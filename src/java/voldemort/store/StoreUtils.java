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

package voldemort.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableFilterIterator;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Check that the given key is valid
 * 
 * 
 */
public class StoreUtils {

    private static Logger logger = Logger.getLogger(StoreUtils.class);

    public static void assertValidKeys(Iterable<?> keys) {
        if(keys == null)
            throw new IllegalArgumentException("Keys cannot be null.");
        for(Object key: keys)
            assertValidKey(key);
    }

    public static <K> void assertValidKey(K key) {
        if(key == null)
            throw new IllegalArgumentException("Key cannot be null.");
    }

    /**
     * Implements get by delegating to getAll.
     */
    public static <K, V, T> List<Versioned<V>> get(Store<K, V, T> storageEngine, K key, T transform) {
        Map<K, List<Versioned<V>>> result = storageEngine.getAll(Collections.singleton(key),
                                                                 Collections.singletonMap(key,
                                                                                          transform));
        if(result.size() > 0)
            return result.get(key);
        else
            return Collections.emptyList();
    }

    /**
     * Implements getAll by delegating to get.
     */
    public static <K, V, T> Map<K, List<Versioned<V>>> getAll(Store<K, V, T> storageEngine,
                                                              Iterable<K> keys,
                                                              Map<K, T> transforms) {
        Map<K, List<Versioned<V>>> result = newEmptyHashMap(keys);
        for(K key: keys) {
            List<Versioned<V>> value = storageEngine.get(key,
                                                         transforms != null ? transforms.get(key)
                                                                           : null);
            if(!value.isEmpty())
                result.put(key, value);
        }
        return result;
    }

    /**
     * Returns an empty map with expected size matching the iterable size if
     * it's of type Collection. Otherwise, an empty map with the default size is
     * returned.
     */
    public static <K, V> HashMap<K, V> newEmptyHashMap(Iterable<?> iterable) {
        if(iterable instanceof Collection<?>)
            return Maps.newHashMapWithExpectedSize(((Collection<?>) iterable).size());
        return Maps.newHashMap();
    }

    /**
     * Closes a Closeable and logs a potential error instead of re-throwing the
     * exception. If {@code null} is passed, this method is a no-op.
     * 
     * This is typically used in finally blocks to prevent an exception thrown
     * during close from hiding an exception thrown inside the try.
     * 
     * @param stream the Closeable to close, may be null.
     */
    public static void close(Closeable c) {
        if(c != null) {
            try {
                c.close();
            } catch(IOException e) {
                logger.error("Error closing stream - " + e.getMessage(), e);
            }
        }
    }

    /**
     * Check if the current node is part of routing request based on cluster.xml
     * or throw an exception.
     * 
     * @param key
     * @param routingStrategy
     * @param currentNodeId
     */
    public static void assertValidMetadata(ByteArray key,
                                           RoutingStrategy routingStrategy,
                                           int currentNode) {
        List<Node> nodes = routingStrategy.routeRequest(key.get());
        for(Node node: nodes) {
            if(node.getId() == currentNode) {
                return;
            }
        }

        throw new InvalidMetadataException("client attempt accessing key belonging to partition:"
                                           + routingStrategy.getPartitionList(key.get())
                                           + " at Node:" + currentNode);
    }

    public static <V> List<Version> getVersions(List<Versioned<V>> versioneds) {
        List<Version> versions = Lists.newArrayListWithCapacity(versioneds.size());
        for(Versioned<?> versioned: versioneds)
            versions.add(versioned.getVersion());
        return versions;
    }

    public static <K, V> ClosableIterator<K> keys(final ClosableIterator<Pair<K, V>> values) {
        return new ClosableIterator<K>() {

            public void close() {
                values.close();
            }

            public boolean hasNext() {
                return values.hasNext();
            }

            public K next() {
                Pair<K, V> value = values.next();
                if(value == null)
                    return null;
                return value.getFirst();
            }

            public void remove() {
                values.remove();
            }

        };
    }

    public static <K> ClosableIterator<K> keys(ClosableIterator<K> iter,
                                               final VoldemortFilter<K, ?> filter) {
        if(filter == null) {
            // If there is no filter, return the input
            return iter;
        } else {
            return new ClosableFilterIterator<K>(iter) {

                @Override
                public boolean matches(K key) {
                    return filter.accept(key, null);
                }
            };
        }
    }

    public static ClosableIterator<ByteArray> keys(ClosableIterator<ByteArray> iter,
                                                   final RoutingStrategy strategy,
                                                   final Collection<Integer> partitions) {
        // If there are no partitions, just return the input value
        if(partitions == null || partitions.size() <= 0) {
            return iter;
        } else {
            return new ClosableFilterIterator<ByteArray>(iter) {

                @Override
                public boolean matches(ByteArray key) {
                    int partition = strategy.getPrimaryPartition(key.get());
                    return partitions.contains(partition);
                }
            };
        }
    }

    public static <K, V> ClosableIterator<Pair<K, Versioned<V>>> entries(ClosableIterator<Pair<K, Versioned<V>>> iter,
                                                                         final VoldemortFilter<K, V> filter) {
        if(filter == null) {
            // If there is no filter, return the input
            return iter;
        } else {
            return new ClosableFilterIterator<Pair<K, Versioned<V>>>(iter) {

                @Override
                public boolean matches(Pair<K, Versioned<V>> entry) {
                    return filter.accept(entry.getFirst(), entry.getSecond());
                }
            };
        }
    }

    public static <V> ClosableIterator<Pair<ByteArray, Versioned<V>>> entries(ClosableIterator<Pair<ByteArray, Versioned<V>>> iter,
                                                                              final RoutingStrategy strategy,
                                                                              final Collection<Integer> partitions) {
        // If there are no partitions, just return the input value
        if(partitions == null || partitions.size() <= 0) {
            return iter;
        } else {
            return new ClosableFilterIterator<Pair<ByteArray, Versioned<V>>>(iter) {

                @Override
                public boolean matches(Pair<ByteArray, Versioned<V>> entry) {
                    int partition = strategy.getPrimaryPartition(entry.getFirst().get());
                    return partitions.contains(partition);
                }
            };
        }
    }

    public static <K, V, T> void deletePartitions(final StorageEngine<K, V, T> engine,
                                                  final Collection<Integer> partitions) {
        ClosableIterator<Pair<K, Versioned<V>>> entries = engine.entries(partitions, null, null);
        try {
            while(entries.hasNext()) {
                try {
                    Pair<K, Versioned<V>> entry = entries.next();
                    K key = entry.getFirst();
                    Versioned<V> versioned = entry.getSecond();
                    engine.delete(key, versioned.getVersion());
                } catch(VoldemortException e) {

                }
            }
        } finally {
            entries.close();
        }

    }

    public static <K, V, T> void deleteEntries(final StorageEngine<K, V, T> engine,
                                               final VoldemortFilter<K, V> filter) {
        ClosableIterator<Pair<K, Versioned<V>>> entries = engine.entries(null, filter, null);
        try {
            while(entries.hasNext()) {
                try {
                    Pair<K, Versioned<V>> entry = entries.next();
                    K key = entry.getFirst();
                    Versioned<V> versioned = entry.getSecond();
                    engine.delete(key, versioned.getVersion());
                } catch(VoldemortException e) {

                }
            }
        } finally {
            entries.close();
        }

    }

    /**
     * This is a temporary measure until we have a type-safe solution for
     * retrieving serializers from a SerializerFactory. It avoids warnings all
     * over the codebase while making it easy to verify who calls it.
     */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> unsafeGetSerializer(SerializerFactory serializerFactory,
                                                        SerializerDefinition serializerDefinition) {
        return (Serializer<T>) serializerFactory.getSerializer(serializerDefinition);
    }

    /**
     * Get a store definition from the given list of store definitions
     * 
     * @param list A list of store definitions
     * @param name The name of the store
     * @return The store definition
     */
    public static StoreDefinition getStoreDef(List<StoreDefinition> list, String name) {
        for(StoreDefinition def: list)
            if(def.getName().equals(name))
                return def;
        return null;
    }
}
