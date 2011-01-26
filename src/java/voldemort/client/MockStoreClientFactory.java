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

package voldemort.client;

import java.io.StringReader;
import java.util.Collections;
import java.util.List;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.NoopFailureDetector;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.store.versioned.VersionIncrementingStore;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.store.views.ViewStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.ChainedResolver;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.TimeBasedInconsistencyResolver;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/**
 * A store client that produces non-persistent, in-memory stores. This is useful
 * for unit testing.
 * 
 * 
 */
@SuppressWarnings("unchecked")
public class MockStoreClientFactory implements StoreClientFactory {

    private final int nodeId;
    private final Serializer<?> keySerializer;
    private final Serializer<?> valueSerializer;
    private final Serializer<?> viewValueSerializer;
    private final Serializer<?> transformsSerializer;
    private final Time time;
    private final FailureDetector failureDetector;
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
    private String storesXml;

    public MockStoreClientFactory(Serializer<?> keySerializer,
                                  Serializer<?> valueSerializer,
                                  Serializer<?> transformsSerializer) {
        this(keySerializer, valueSerializer, null, transformsSerializer, 0, SystemTime.INSTANCE);
    }

    public MockStoreClientFactory(Serializer<?> keySerializer,
                                  Serializer<?> valueSerializer,
                                  Serializer<?> viewValueSerializer,
                                  Serializer<?> transformsSerializer,
                                  String storesXml) {
        this(keySerializer,
             valueSerializer,
             viewValueSerializer,
             transformsSerializer,
             0,
             SystemTime.INSTANCE);
        this.storesXml = storesXml;
    }

    public MockStoreClientFactory(Serializer<?> keySerializer,
                                  Serializer<?> valueSerializer,
                                  Serializer<?> viewValueSerializer,
                                  Serializer<?> transformsSerializer,
                                  int nodeId,
                                  Time time) {
        this.nodeId = nodeId;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.viewValueSerializer = viewValueSerializer;
        this.transformsSerializer = transformsSerializer;
        this.time = time;
        failureDetector = new NoopFailureDetector();
    }

    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        return getStoreClient(storeName, new TimeBasedInconsistencyResolver<V>());
    }

    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> resolver) {
        Store<K, V, Object> metadataStore = new MetadataRefreshingStore<K, V, Object>(storeName,
                                                                                      resolver,
                                                                                      this,
                                                                                      3);
        return new DefaultStoreClient<K, V>(metadataStore);
    }

    public <K1, V1, T1> Store<K1, V1, T1> getRawStore(String storeName,
                                                      InconsistencyResolver<Versioned<V1>> resolver) {
        if(this.storesXml != null)
            return getRawStore(storeName);

        // Add inconsistency resolving decorator, using their inconsistency
        // resolver (if they gave us one)
        InconsistencyResolver<Versioned<V1>> secondaryResolver = new TimeBasedInconsistencyResolver();
        if(resolver != null)
            secondaryResolver = resolver;

        Store store = new VersionIncrementingStore(new InMemoryStore(storeName), nodeId, time);
        if(isSerialized())
            store = new SerializingStore(store,
                                         keySerializer,
                                         valueSerializer,
                                         transformsSerializer);

        Store<K1, V1, T1> consistentStore = new InconsistencyResolvingStore<K1, V1, T1>(store,
                                                                                        new ChainedResolver<Versioned<V1>>(new VectorClockInconsistencyResolver(),
                                                                                                                           secondaryResolver));
        return consistentStore;
    }

    public Cluster getCluster() {
        Node node = new Node(nodeId, "localhost", 6666, 8081, 6667, Lists.newArrayList(0, 1));
        return new Cluster("mock", Collections.singletonList(node));
    }

    private StorageEngine<ByteArray, byte[], byte[]> getMemoryEngine(StoreDefinition storeDef) {
        List<StoreDefinition> storeDefs = storeMapper.readStoreList(new StringReader(storesXml));
        Cluster cluster = getCluster();

        Store<String, String, String> innerStore = InMemoryStore.create("inner-metadata");
        innerStore.put(MetadataStore.CLUSTER_KEY,
                       new Versioned<String>(new ClusterMapper().writeCluster(cluster)),
                       null);
        innerStore.put(MetadataStore.STORES_KEY,
                       new Versioned<String>(new StoreDefinitionsMapper().writeStoreList(storeDefs)),
                       null);
        MetadataStore metadata = new MetadataStore(innerStore, this.nodeId);
        Props props = new Props().with("voldemort.home", System.getProperty("java.io.tmpdir"));
        VoldemortConfig config = new VoldemortConfig(props, metadata);
        InMemoryStorageConfiguration storeConfiguration = new InMemoryStorageConfiguration(config);
        return storeConfiguration.getStore(storeDef);
    }

    private <K1, V1, T1> Store<K1, V1, T1> getRawStore(String storeName) {
        List<StoreDefinition> storeDefs = storeMapper.readStoreList(new StringReader(storesXml));
        StoreDefinition storeDef = null;
        for(StoreDefinition d: storeDefs)
            if(d.getName().equals(storeName))
                storeDef = d;
        if(storeDef == null)
            throw new BootstrapFailureException("Unknown store '" + storeName + "'.");

        DefaultSerializerFactory serializerFactory = new DefaultSerializerFactory();

        Serializer<K1> keySerializer = (Serializer<K1>) serializerFactory.getSerializer(storeDef.getKeySerializer());
        Serializer<V1> valueSerializer = (Serializer<V1>) serializerFactory.getSerializer(storeDef.getValueSerializer());
        Serializer<T1> transformsSerializer = null;

        if(storeDef.isView())
            transformsSerializer = (Serializer<T1>) serializerFactory.getSerializer(storeDef.getTransformsSerializer());

        // Add inconsistency resolving decorator, using their inconsistency
        // resolver (if they gave us one)
        InconsistencyResolver<Versioned<V1>> secondaryResolver = new TimeBasedInconsistencyResolver<V1>();

        StorageEngine engine;
        if(storeDef.isView()) {
            String targetName = storeDef.getViewTargetStoreName();
            StoreDefinition targetDef = StoreUtils.getStoreDef(storeDefs, targetName);
            engine = getMemoryEngine(targetDef);
            // instantiate view

            engine = new ViewStorageEngine(storeDef,
                                           engine,
                                           this.viewValueSerializer != null ? this.viewValueSerializer
                                                                           : serializerFactory.getSerializer(storeDef.getValueSerializer()),
                                           this.transformsSerializer != null ? this.transformsSerializer
                                                                            : serializerFactory.getSerializer(storeDef.getTransformsSerializer()),
                                           this.keySerializer != null ? this.keySerializer
                                                                     : serializerFactory.getSerializer(targetDef.getKeySerializer()),
                                           this.valueSerializer != null ? this.valueSerializer
                                                                       : serializerFactory.getSerializer(targetDef.getValueSerializer()),
                                           null,
                                           ViewStorageConfiguration.loadTransformation(storeDef.getValueTransformation()));
        } else {
            engine = getMemoryEngine(storeDef);
        }

        Store store = new VersionIncrementingStore(engine, nodeId, time);

        store = new SerializingStore(store,
                                     this.keySerializer != null ? this.keySerializer
                                                               : keySerializer,
                                     this.valueSerializer != null ? this.valueSerializer
                                                                 : valueSerializer,
                                     this.transformsSerializer != null ? this.transformsSerializer
                                                                      : transformsSerializer);

        Store<K1, V1, T1> consistentStore = new InconsistencyResolvingStore<K1, V1, T1>(store,
                                                                                        new ChainedResolver<Versioned<V1>>(new VectorClockInconsistencyResolver(),
                                                                                                                           secondaryResolver));
        return consistentStore;
    }

    private boolean isSerialized() {
        return keySerializer != null && valueSerializer != null;
    }

    public void close() {

    }

    public FailureDetector getFailureDetector() {
        return failureDetector;
    }

}
