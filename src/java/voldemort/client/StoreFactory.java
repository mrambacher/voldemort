package voldemort.client;

import java.util.Map;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.utils.ByteArray;

public interface StoreFactory {

    public AsynchronousStore<ByteArray, byte[], byte[]> getNodeStore(Node node,
                                                                     StoreDefinition storeDef);

    public AsynchronousStore<ByteArray, byte[], byte[]> getNodeStore(Node node, String storeName);

    public Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> getNodeStores(StoreDefinition storeDef);

    public Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> getNodeStores(String storeName);

    public <K, V, T> Store<K, V, T> getStore(String storeName);

    public <K, V, T> Store<K, V, T> getStore(StoreDefinition storeDef);

    public StoreDefinition getStoreDefinition(String storeName);

    public Cluster getCluster();
}
