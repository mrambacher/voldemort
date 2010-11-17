package voldemort.store.routed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.distributed.DelegatingDistributedStore;
import voldemort.store.distributed.DistributedStore;
import voldemort.utils.ByteArray;

public class RoutableStore<V> extends DelegatingDistributedStore<Node, ByteArray, V> {

    private final FailureDetector failureDetector;
    protected volatile RoutingStrategy routingStrategy;
    protected final Logger logger = Logger.getLogger(getClass());

    public RoutableStore(DistributedStore<Node, ByteArray, V> inner,
                         Cluster cluster,
                         StoreDefinition storeDef,
                         FailureDetector detector) {
        super(inner);
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
        this.failureDetector = detector;
    }

    public void updateRoutingStrategy(RoutingStrategy routingStrategy) {
        logger.info("Updating routing strategy for RoutedStore:" + getName());
        this.routingStrategy = routingStrategy;
    }

    @Override
    public Collection<Node> getNodesForKey(final ByteArray key) {
        return availableNodes(routingStrategy.routeRequest(key.get()));
    }

    private List<Node> availableNodes(List<Node> list) {
        List<Node> available = new ArrayList<Node>(list.size());
        for(Node node: list)
            if(failureDetector.isAvailable(node))
                available.add(node);
        return available;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case ROUTING_STRATEGY:
                return this.routingStrategy;
            case FAILURE_DETECTOR:
                return this.failureDetector;
            default:
                return super.getCapability(capability);
        }
    }
}
