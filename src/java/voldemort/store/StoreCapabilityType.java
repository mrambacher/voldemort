package voldemort.store;

public enum StoreCapabilityType {
    KEY_SERIALIZER,
    VALUE_SERIALIZER,
    ROUTING_STRATEGY,
    STAT_TRACKER,
    READ_REPAIRER,
    INCONSISTENCY_RESOLVER,
    LOGGER,
    SOCKET_POOL,
    VERSION_INCREMENTING,
    VIEW_TARGET,
    ROLLBACK_FROM_BACKUP,
    FAILURE_DETECTOR,
    NODE,
    NODE_STORES,
    SYNCHRONOUS_NODE_STORES,
    ASYNCHRONOUS,
    BOOT_STRAP;
}
