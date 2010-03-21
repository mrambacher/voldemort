package voldemort.routing;

/**
 * An enumeration of RoutingStrategies type
 * 
 * 
 */
public class RoutingStrategyType {

    public static final String CONSISTENT_STRATEGY = "consistent-routing";
    public static final String TO_ALL_STRATEGY = "all-routing";

    private final String name;

    private RoutingStrategyType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
