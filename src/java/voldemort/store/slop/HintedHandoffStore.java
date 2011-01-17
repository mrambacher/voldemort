package voldemort.store.slop;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SlopSerializer;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.StoreDefinition;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.distributed.AsynchronousDistributedStore;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.distributed.DistributedStoreFactory;
import voldemort.store.distributed.ResultsBuilder;
import voldemort.store.slop.strategy.HintedHandoffStrategy;
import voldemort.store.slop.strategy.HintedHandoffStrategyFactory;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class HintedHandoffStore extends
        AsynchronousDistributedStore<Node, ByteArray, byte[], byte[]> {

    private static final Serializer<Slop> slopSerializer = new SlopSerializer();
    private static final Serializer<byte[]> serializer = new IdentitySerializer();
    private final Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> slopStores;
    private final FailureDetector failureDetector;
    final HintedHandoffStrategy strategy;
    final HintedHandoffStrategyFactory factory;
    protected final ResultsBuilder<Node, Version> putBuilder;
    protected final ResultsBuilder<Node, Boolean> deleteBuilder;

    public HintedHandoffStore(DistributedStore<Node, ByteArray, byte[], byte[]> inner,
                              StoreDefinition storeDef,
                              Cluster cluster,
                              Map<Node, AsynchronousStore<ByteArray, byte[], byte[]>> slopStores,
                              FailureDetector failureDetector,
                              int zoneId) {
        super(storeDef, inner);
        this.slopStores = slopStores;
        this.failureDetector = failureDetector;
        this.putBuilder = DistributedStoreFactory.PutBuilder();
        this.deleteBuilder = DistributedStoreFactory.DeleteBuilder();
        factory = new HintedHandoffStrategyFactory(storeDef.isZoneRoutingEnabled(), zoneId);
        this.strategy = factory.updateHintedHandoffStrategy(storeDef, cluster);
    }

    public List<Node> routeHint(Node origin) {
        return strategy.routeHint(origin);
    }

    public boolean isAvailable(Node node) {
        return failureDetector.isAvailable(node);
    }

    private AsynchronousStore<ByteArray, byte[], byte[]> getSlopStore(Node node) {
        AsynchronousStore<ByteArray, byte[], byte[]> async = this.slopStores.get(node);
        return async;
    }

    public StoreFuture<Version> submitSlop(Node node, Slop slop, Version version) {
        AsynchronousStore<ByteArray, byte[], byte[]> slopStore = this.getSlopStore(node);
        Versioned<byte[]> slopVersioned = new Versioned<byte[]>(slopSerializer.toBytes(slop),
                                                                version);
        return slopStore.submitPut(slop.makeKey(), slopVersioned, null);
    }

    public boolean storeSlopToNode(Node slopNode, Slop slop, Version version) {
        boolean saved = false;
        try {
            StoreFuture<Version> future = submitSlop(slopNode, slop, version);
            future.get();
            saved = true;
        } catch(VoldemortException e) {}
        return saved;
    }

    public boolean storeSlop(Node node, Collection<Node> skip, Slop slop, Version version) {
        for(final Node slopNode: routeHint(node)) {
            if(!skip.contains(slopNode) && isAvailable(slopNode)) {
                skip.add(slopNode);
                if(storeSlopToNode(slopNode, slop, version)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public DistributedFuture<Node, Version> submitPut(final ByteArray key,
                                                      final Versioned<byte[]> value,
                                                      final byte[] transform,
                                                      final Collection<Node> nodes,
                                                      int preferred,
                                                      int required) {
        DistributedFuture<Node, Version> future = super.submitPut(key,
                                                                  value,
                                                                  transform,
                                                                  nodes,
                                                                  preferred,
                                                                  required);
        // **TODO: This is what it should be:
        // byte[] data = VersionFactory.toBytes(value, serializer);
        byte[] data = value.getValue();
        return new HintedHandoffFuture<Version>(VoldemortOpCode.PUT.getMethodName(),
                                                future,
                                                putBuilder,
                                                Slop.Operation.PUT,
                                                key,
                                                data,
                                                transform,
                                                value.getVersion(),
                                                this,
                                                value.getVersion());
    }

    @Override
    public DistributedFuture<Node, Boolean> submitDelete(final ByteArray key,
                                                         final Version version,
                                                         final Collection<Node> nodes,
                                                         int preferred,
                                                         int required) {
        DistributedFuture<Node, Boolean> future = super.submitDelete(key,
                                                                     version,
                                                                     nodes,
                                                                     preferred,
                                                                     required);
        return new HintedHandoffFuture<Boolean>(VoldemortOpCode.DELETE.getMethodName(),
                                                future,
                                                deleteBuilder,
                                                Slop.Operation.DELETE,
                                                key,
                                                version.toBytes(),
                                                null,
                                                version,
                                                this,
                                                false);
    }
}
