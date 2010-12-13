package voldemort.store.slop;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.async.StoreFuture;
import voldemort.store.async.StoreFutureListener;
import voldemort.store.distributed.AbstractDistributedFuture;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.ResultsBuilder;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;

import com.google.common.collect.Maps;

public class HintedHandoffFuture<V> extends AbstractDistributedFuture<Node, V> {

    /** The set of futures associated with this one. */
    protected Map<Node, StoreFuture<V>> futures;
    private StoreFutureListener<V> listener;

    private final HintedHandoffStore handoff;
    private final V result;
    private Collection<Node> visited;
    private long handoffTime = 0;

    public HintedHandoffFuture(String operation,
                               DistributedFuture<Node, V> distributed,
                               ResultsBuilder<Node, V> builder,
                               final Slop.Operation slopCommand,
                               final ByteArray key,
                               final byte[] value,
                               final byte[] transform,
                               final Version version,
                               final HintedHandoffStore handoff,
                               final V handoffResult) {
        super(operation,
              builder,
              distributed.getAvailable(),
              distributed.getPreferred(),
              distributed.getRequired());
        this.futures = Maps.newHashMap();
        this.handoff = handoff;
        this.result = handoffResult;
        this.visited = Collections.synchronizedCollection(new HashSet<Node>());
        visited.addAll(distributed.getNodes());
        this.listener = new StoreFutureListener<V>() {

            public void futureCompleted(Object arg, V result, long duration) {
                Node node = (Node) arg;
                nodeCompleted(node, result, duration);
            }

            public void futureFailed(Object arg, VoldemortException exception, long duration) {
                Node node = (Node) arg;
                Slop slop = new Slop(handoff.getName(),
                                     slopCommand,
                                     key,
                                     value,
                                     transform,
                                     node.getId(),
                                     new Date());
                if(!submitSlop(node, slop, version)) {
                    nodeFailed(node, exception, duration);
                }
            }
        };
        for(Node node: distributed.getNodes()) {
            StoreFuture<V> future = distributed.getFuture(node);
            futures.put(node, future);
            future.register(listener, node);
        }
    }

    private boolean submitSlop(Node node, final Slop slop, final Version version) {
        for(final Node slopNode: handoff.routeHint(node)) {
            if(!visited.contains(slopNode) && handoff.isAvailable(slopNode)) {
                visited.add(slopNode);
                handoffTime = System.nanoTime();
                StoreFuture<Version> slopFuture = handoff.submitSlop(slopNode, slop, version);
                slopFuture.register(new StoreFutureListener<Version>() {

                    public void futureCompleted(Object key, Version version, long duration) {
                        Node node = (Node) key;
                        nodeCompleted(node, result, duration);
                    }

                    public void futureFailed(Object key, VoldemortException ex, long duration) {
                        Node node = (Node) key;
                        if(!submitSlop(node, slop, version)) {
                            handoffTime = 0;
                            nodeFailed(node, ex, duration);
                        }
                    }
                }, node);
                return true;
            }
        }
        return false;
    }

    public Collection<Node> getNodes() {
        return futures.keySet();
    }

    public StoreFuture<V> getFuture(Node node) {
        return futures.get(node);
    }

    @Override
    public long getRemaining(long timeout, TimeUnit units) {
        if(handoffTime > 0) {
            return Math.max(0, timeout
                               - units.convert(System.nanoTime() - handoffTime,
                                               TimeUnit.NANOSECONDS));
        } else {
            return super.getRemaining(timeout, units);
        }
    }
}
