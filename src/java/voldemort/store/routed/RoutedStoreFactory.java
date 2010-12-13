package voldemort.store.routed;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.StoreDefinition;
import voldemort.store.distributed.DistributedStore;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;

public class RoutedStoreFactory {

    private final boolean isPipelineRoutedStoreEnabled;

    private final FailureDetector failureDetector;

    private final long routingTimeoutMs;

    public RoutedStoreFactory(boolean isPipelineRoutedStoreEnabled,
                              FailureDetector detector,
                              long routingTimeoutMs) {
        this.isPipelineRoutedStoreEnabled = isPipelineRoutedStoreEnabled;
        this.failureDetector = detector;
        this.routingTimeoutMs = routingTimeoutMs;
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              DistributedStore<Node, ByteArray, byte[], byte[]> distributor,
                              int clientZoneId) {
        if(isPipelineRoutedStoreEnabled) {
            return new PipelineRoutedStore(storeDefinition.getName(),
                                           distributor,
                                           cluster,
                                           storeDefinition,
                                           clientZoneId,
                                           routingTimeoutMs,
                                           failureDetector);
        } else {
            return new ThreadPoolRoutedStore(storeDefinition.getName(),
                                             distributor,
                                             cluster,
                                             storeDefinition,
                                             failureDetector,
                                             routingTimeoutMs,
                                             SystemTime.INSTANCE);
        }
    }

    public RoutedStore create(Cluster cluster,
                              StoreDefinition storeDefinition,
                              DistributedStore<Node, ByteArray, byte[], byte[]> distributor) {
        return create(cluster, storeDefinition, distributor, Zone.DEFAULT_ZONE_ID);
    }

}
