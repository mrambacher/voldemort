package voldemort.store.stats;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;

import com.google.common.collect.ImmutableList;

/**
 * A wrapper class to expose store stats via JMX
 * 
 * 
 */
@JmxManaged
public class StoreStatsJmx {

    private StoreStats stats;

    public StoreStatsJmx(StoreStats stats) {
        this.stats = stats;
    }

    @JmxGetter(name = "numberOfCallsToGetAll", description = "The number of calls to GET_ALL since the last reset.")
    public long getNumberOfCallsToGetAll() {
        return stats.getCount(Tracked.GET_ALL);
    }

    @JmxGetter(name = "averageGetAllCompletionTimeInMs", description = "The avg. time in ms for GET_ALL calls to complete.")
    public double getAverageGetAllCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "GetAllThroughput", description = "Throughput of GET_ALL requests.")
    public float getGetAllThroughput() {
        return stats.getThroughput(Tracked.GET_ALL);
    }

    @JmxGetter(name = "numberOfCallsToGet", description = "The number of calls to GET since the last reset.")
    public long getNumberOfCallsToGet() {
        return stats.getCount(Tracked.GET);
    }

    @JmxGetter(name = "averageGetCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageGetCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET);
    }

    @JmxGetter(name = "GetThroughput", description = "Throughput of GET requests.")
    public float getGetThroughput() {
        return stats.getThroughput(Tracked.GET);
    }

    @JmxGetter(name = "numberOfCallsToPut", description = "The number of calls to PUT since the last reset.")
    public long getNumberOfCallsToPut() {
        return stats.getCount(Tracked.PUT);
    }

    @JmxGetter(name = "averagePutCompletionTimeInMs", description = "The avg. time in ms for PUT calls to complete.")
    public double getAveragePutCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.PUT);
    }

    @JmxGetter(name = "PutThroughput", description = "Throughput of PUT requests.")
    public float getPutThroughput() {
        return stats.getThroughput(Tracked.PUT);
    }

    @JmxGetter(name = "numberOfCallsToDelete", description = "The number of calls to DELETE since the last reset.")
    public long getNumberOfCallsToDelete() {
        return stats.getCount(Tracked.DELETE);
    }

    @JmxGetter(name = "averageDeleteCompletionTimeInMs", description = "The avg. time in ms for DELETE calls to complete.")
    public double getAverageDeleteCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.DELETE);
    }

    @JmxGetter(name = "DeleteThroughput", description = "Throughput of DELETE requests.")
    public float getDeleteThroughput() {
        return stats.getThroughput(Tracked.DELETE);
    }

    @JmxGetter(name = "numberOfObsoleteVersions", description = "Number of ObsoleteVersionExceptions since the last reset.")
    public long getNumberOfObsoleteVersions() {
        return stats.getCount(Tracked.OBSOLETE);
    }

    @JmxGetter(name = "numberOfExceptions", description = "The number of exceptions since the last reset.")
    public long getNumberOfExceptions() {
        return stats.getCount(Tracked.EXCEPTION);
    }

    @JmxGetter(name = "averageOperationTimeInMs", description = "The total nuber of all operations that have occured.")
    public double getAvgOperationCompletionTimeInMs() {
        double sum = 0.0;
        double weightedTime = 0.0;
        for(Tracked stat: ImmutableList.of(Tracked.DELETE,
                                           Tracked.GET,
                                           Tracked.GET_ALL,
                                           Tracked.PUT)) {
            sum += stats.getCount(stat);
            weightedTime += stats.getCount(stat) * stats.getAvgTimeInMs(stat);
        }
        if(sum <= 0)
            return 0;
        else
            return weightedTime / sum;
    }

    @JmxGetter(name = "AllOperationThroughput", description = "The number of exceptions since the last reset.")
    public double getOperationThroughput() {
        return stats.getThroughput(Tracked.DELETE) + stats.getThroughput(Tracked.GET)
               + stats.getThroughput(Tracked.GET_ALL) + stats.getThroughput(Tracked.PUT);
    }

    // -----------------------------------------------------------------------------
    // PUT Percentiles -
    // Note the dot "." in the JMX's name sort the methods names in ascending
    // order
    // -----------------------------------------------------------------------------
    @JmxGetter(name = "Put50.0PercentileInMs", description = "PUT for 50 percentile")
    public String getPut50PercentileInMs() {
        return stats.getLatencyStats(Tracked.PUT, 0.50F);
    }

    @JmxGetter(name = "Put90.0PercentileInMs", description = "PUT for 90 percentile")
    public String getPut90PercentileInMs() {
        return stats.getLatencyStats(Tracked.PUT, 0.90F);
    }

    @JmxGetter(name = "Put99.0PercentileInMs", description = "PUT for 99 percentile")
    public String getPut99PercentileInMs() {
        return stats.getLatencyStats(Tracked.PUT, 0.99F);
    }

    @JmxGetter(name = "Put99.5PercentileInMs", description = "PUT for 99.5 percentile")
    public String getPut995PercentileInMs() {
        return stats.getLatencyStats(Tracked.PUT, 0.995F);
    }

    @JmxGetter(name = "Put99.9PercentileInMs", description = "PUT for 99.9 percentile")
    public String getPut999PercentileInMs() {
        return stats.getLatencyStats(Tracked.PUT, 0.999F);
    }

    // -----------------------------------------------------------------------------
    // GET Percentiles
    // Note the dot "." in the JMX's name sort the methods names in ascending
    // order
    // -----------------------------------------------------------------------------
    @JmxGetter(name = "Get50.0PercentileInMs", description = "GET for 50 percentile")
    public String getGet50PercentileInMs() {
        return stats.getLatencyStats(Tracked.GET, 0.50F);
    }

    @JmxGetter(name = "Get90.0PercentileInMs", description = "GET for 90 percentile")
    public String getGet90PercentileInMs() {
        return stats.getLatencyStats(Tracked.GET, 090F);
    }

    @JmxGetter(name = "Get99.0PercentileInMs", description = "GET for 99 percentile")
    public String getGet99PercentileInMs() {
        return stats.getLatencyStats(Tracked.GET, 0.99F);
    }

    @JmxGetter(name = "Get99.5PercentileInMs", description = "GET for 99.5 percentile")
    public String getGet995PercentileInMs() {
        return stats.getLatencyStats(Tracked.GET, 0.995F);
    }

    @JmxGetter(name = "Get99.9PercentileInMs", description = "GET for 99.9 percentile")
    public String getGet999PercentileInMs() {
        return stats.getLatencyStats(Tracked.GET, 0.999F);
    }

    // -----------------------------------------------------------------------------
    // DELETE Percentiles
    // Note the dot "." in the JMX's name sort the methods names in ascending
    // order
    // -----------------------------------------------------------------------------
    @JmxGetter(name = "Delete50.0PercentileInMs", description = "DELETE for 50 percentile")
    public String getDelete50PercentileInMs() {
        return stats.getLatencyStats(Tracked.DELETE, 0.50F);
    }

    @JmxGetter(name = "Delete90.0PercentileInMs", description = "DELETE for 90 percentile")
    public String getDelete90PercentileInMs() {
        return stats.getLatencyStats(Tracked.DELETE, 090F);
    }

    @JmxGetter(name = "Delete99.0PercentileInMs", description = "DELETE for 99 percentile")
    public String getDelete99PercentileInMs() {
        return stats.getLatencyStats(Tracked.DELETE, 0.99F);
    }

    @JmxGetter(name = "Delete99.5PercentileInMs", description = "DELETE for 99.5 percentile")
    public String getDelete995PercentileInMs() {
        return stats.getLatencyStats(Tracked.DELETE, 0.995F);
    }

    @JmxGetter(name = "Delete99.9PercentileInMs", description = "DELETE for 99.9 percentile")
    public String getDelete999PercentileInMs() {
        return stats.getLatencyStats(Tracked.DELETE, 0.999F);
    }

    @JmxGetter(name = "numberOfInconsistentGet", description = "The number of inconsistent calls to GET or GET_ALL since the last reset.")
    public long getNumberOfInconsistentGet() {
        return stats.getCount(Tracked.INCONSISTENT_GET);
    }
}
