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
import voldemort.versioning.Version;

public class PerformParallelGetVersionsRequest extends
        AbstractKeyBasedAction<ByteArray, List<Version>, BasicPipelineData<List<Version>>> {

    private final int preferred;

    private final int required;

    private final long timeoutMs;

    private final DistributedStore<Node, ByteArray, byte[], byte[]> distributor;

    public PerformParallelGetVersionsRequest(BasicPipelineData<List<Version>> pipelineData,
                                             Event completeEvent,
                                             ByteArray key,
                                             int preferred,
                                             int required,
                                             long timeoutMs,
                                             DistributedStore<Node, ByteArray, byte[], byte[]> distributor) {
        super(pipelineData, completeEvent, key);
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.distributor = distributor;
    }

    public void execute(final Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();
        int attempts = Math.min(preferred, nodes.size());

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        try {
            DistributedFuture<Node, List<Version>> future = distributor.submitGetVersions(key,
                                                                                          nodes,
                                                                                          attempts,
                                                                                          this.required);
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
            Map<Node, List<Version>> results = future.getResults();
            for(Map.Entry<Node, List<Version>> result: results.entrySet()) {
                Node node = result.getKey();
                pipelineData.getResponses()
                            .add(new Response<ByteArray, List<Version>>(node,
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
