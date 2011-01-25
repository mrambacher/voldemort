package voldemort.client.protocol.admin.filter;

import java.util.List;

import voldemort.client.protocol.VoldemortFilter;
import voldemort.routing.RoutingStrategy;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class MasterOnlyVoldemortFilter<V> implements VoldemortFilter<ByteArray, V> {

    RoutingStrategy routingStrategy = null;
    List<Integer> partitionsList = null;

    public MasterOnlyVoldemortFilter(RoutingStrategy routingStrategy, List<Integer> partitionsList) {
        this.routingStrategy = Utils.notNull(routingStrategy);
        this.partitionsList = Utils.notNull(partitionsList);
    }

    public boolean accept(ByteArray key, Versioned<V> value) {
        List<Integer> partitionIds = this.routingStrategy.getPartitionList(key.get());
        if(partitionsList.contains(partitionIds.get(0))) {
            return true;
        }
        return false;
    }

}
