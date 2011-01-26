package voldemort.cluster;

public interface ClusterListener {

    public void clusterUpdated(Cluster cluster);
}
