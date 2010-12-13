package voldemort.store.routed.action;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.distributed.DistributedFuture;
import voldemort.store.distributed.DistributedStore;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class PerformParallelGetRequests
        extends
        AbstractKeyBasedAction<ByteArray, List<Versioned<byte[]>>, BasicPipelineData<List<Versioned<byte[]>>>> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final byte[] transforms;
    private final DistributedStore<Node, ByteArray, byte[], byte[]> distributor;

    public PerformParallelGetRequests(BasicPipelineData<List<Versioned<byte[]>>> pipelineData,
                                      Event completeEvent,
                                      ByteArray key,
                                      byte[] transforms,
                                      int preferred,
                                      int required,
                                      long timeoutMs,
                                      DistributedStore<Node, ByteArray, byte[], byte[]> distributor) {
        super(pipelineData, completeEvent, key);
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.distributor = distributor;
        this.transforms = transforms;
    }

    public void execute(final Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();
        int attempts = Math.min(preferred, nodes.size());

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        try {
            DistributedFuture<Node, List<Versioned<byte[]>>> future = distributor.submitGet(key,
                                                                                            transforms,
                                                                                            nodes,
                                                                                            attempts,
                                                                                            this.required);
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
            Map<Node, List<Versioned<byte[]>>> results = future.getResults();
            for(Map.Entry<Node, List<Versioned<byte[]>>> result: results.entrySet()) {
                Node node = result.getKey();
                pipelineData.getResponses()
                            .add(new Response<ByteArray, List<Versioned<byte[]>>>(node,
                                                                                  key,
                                                                                  result.getValue(),
                                                                                  future.getDuration(node,
                                                                                                     TimeUnit.MILLISECONDS)));
                pipelineData.getZoneResponses().add(node.getZoneId());
                pipelineData.incrementSuccesses();
            }
            pipeline.addEvent(completeEvent);
        } catch(VoldemortException ex) {
            pipelineData.setFatalError(ex);
            pipeline.addEvent(Event.ERROR);
        }
    }

}
