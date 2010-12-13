/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store;

import java.util.HashMap;
import java.util.Properties;

import voldemort.client.RoutingTier;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.utils.Props;
import voldemort.utils.Utils;

/**
 * A simple builder class to avoid having 10k constructor parameters in store
 * definitions
 * 
 * 
 */
public class StoreDefinitionBuilder {

    private String name = null;
    private String type = null;
    private SerializerDefinition keySerializer = null;
    private SerializerDefinition valueSerializer = null;
    private SerializerDefinition transformsSerializer = null;
    private RoutingTier routingPolicy = null;
    private int replicationFactor = -1;
    private Integer preferredWrites = null;
    private int requiredWrites = -1;
    private Integer preferredReads = null;
    private int requiredReads = -1;
    private Integer retentionPeriodDays = null;
    private Integer retentionScanThrottleRate = null;
    private String routingStrategyType = null;
    private String viewOf = null;
    private HashMap<Integer, Integer> zoneReplicationFactor = null;
    private Integer zoneCountReads;
    private Integer zoneCountWrites;
    private String view = null;
    private String serializerFactory = null;
    private HintedHandoffStrategyType hintedHandoffStrategy = null;
    private Integer hintPrefListSize = null;

    private Props properties = new Props();

    public String getName() {
        return Utils.notNull(name);
    }

    public StoreDefinitionBuilder setName(String name) {
        this.name = Utils.notNull(name);
        return this;
    }

    public String getType() {
        return Utils.notNull(type);
    }

    public StoreDefinitionBuilder setType(String type) {
        this.type = Utils.notNull(type);
        return this;
    }

    public SerializerDefinition getKeySerializer() {
        return Utils.notNull(keySerializer);
    }

    public StoreDefinitionBuilder setKeySerializer(SerializerDefinition keySerializer) {
        this.keySerializer = Utils.notNull(keySerializer);
        return this;
    }

    public SerializerDefinition getValueSerializer() {
        return Utils.notNull(valueSerializer);
    }

    public StoreDefinitionBuilder setValueSerializer(SerializerDefinition valueSerializer) {
        this.valueSerializer = Utils.notNull(valueSerializer);
        return this;
    }

    public SerializerDefinition getTransformsSerializer() {
        return this.transformsSerializer;
    }

    public StoreDefinitionBuilder setTransformsSerializer(SerializerDefinition transformsSerializer) {
        this.transformsSerializer = transformsSerializer;
        return this;
    }

    public RoutingTier getRoutingPolicy() {
        return Utils.notNull(routingPolicy);
    }

    public StoreDefinitionBuilder setRoutingPolicy(RoutingTier routingPolicy) {
        this.routingPolicy = Utils.notNull(routingPolicy);
        return this;
    }

    public int getReplicationFactor() {
        return Utils.inRange(replicationFactor, 1, Integer.MAX_VALUE);
    }

    public StoreDefinitionBuilder setReplicationFactor(int replicationFactor) {
        this.replicationFactor = Utils.inRange(replicationFactor, 1, Integer.MAX_VALUE);
        return this;
    }

    public boolean hasPreferredWrites() {
        return preferredWrites != null;
    }

    public Integer getPreferredWrites() {
        return preferredWrites;
    }

    public StoreDefinitionBuilder setPreferredWrites(Integer preferredWrites) {
        this.preferredWrites = preferredWrites;
        return this;
    }

    public int getRequiredWrites() {
        return requiredWrites;
    }

    public StoreDefinitionBuilder setRequiredWrites(int requiredWrites) {
        this.requiredWrites = Utils.inRange(requiredWrites, 0, Integer.MAX_VALUE);
        return this;
    }

    public boolean hasPreferredReads() {
        return preferredReads != null;
    }

    public Integer getPreferredReads() {
        return preferredReads;
    }

    public StoreDefinitionBuilder setPreferredReads(Integer preferredReads) {
        this.preferredReads = preferredReads;
        return this;
    }

    public int getRequiredReads() {
        return requiredReads;
    }

    public StoreDefinitionBuilder setRequiredReads(int requiredReads) {
        this.requiredReads = Utils.inRange(requiredReads, 0, Integer.MAX_VALUE);
        return this;
    }

    public Integer getRetentionPeriodDays() {
        return retentionPeriodDays;
    }

    public StoreDefinitionBuilder setRetentionPeriodDays(Integer retentionPeriodDays) {
        this.retentionPeriodDays = retentionPeriodDays;
        return this;
    }

    public boolean hasRetentionScanThrottleRate() {
        return this.retentionScanThrottleRate != null;
    }

    public Integer getRetentionScanThrottleRate() {
        return retentionScanThrottleRate;
    }

    public StoreDefinitionBuilder setRetentionScanThrottleRate(Integer retentionScanThrottleRate) {
        this.retentionScanThrottleRate = retentionScanThrottleRate;
        return this;
    }

    public String getRoutingStrategyType() {
        return routingStrategyType;
    }

    public StoreDefinitionBuilder setRoutingStrategyType(String routingStrategyType) {
        this.routingStrategyType = Utils.notNull(routingStrategyType);
        return this;
    }

    public Props getProperties() {
        return this.properties;
    }

    public StoreDefinitionBuilder setProperties(Props props) {
        this.properties = props;
        return this;
    }

    public StoreDefinitionBuilder setProperties(Properties properties) {
        Props props = new Props(properties);
        return setProperties(props);
    }

    public boolean isView() {
        return viewOf != null;
    }

    public String getViewOf() {
        return viewOf;
    }

    public StoreDefinitionBuilder setViewOf(String viewOf) {
        this.viewOf = Utils.notNull(viewOf);
        return this;
    }

    public String getView() {
        return view;
    }

    public StoreDefinitionBuilder setView(String valueTransformation) {
        this.view = valueTransformation;
        return this;
    }

    public String getSerializerFactory() {
        return this.serializerFactory;
    }

    public StoreDefinitionBuilder setSerializerFactory(String factory) {
        this.serializerFactory = factory;
        return this;
    }

    public HashMap<Integer, Integer> getZoneReplicationFactor() {
        return zoneReplicationFactor;
    }

    public StoreDefinitionBuilder setZoneReplicationFactor(HashMap<Integer, Integer> zoneReplicationFactor) {
        this.zoneReplicationFactor = zoneReplicationFactor;
        return this;
    }

    public Integer getZoneCountReads() {
        return zoneCountReads;
    }

    public StoreDefinitionBuilder setZoneCountReads(Integer zoneCountReads) {
        this.zoneCountReads = zoneCountReads;
        return this;
    }

    public Integer getZoneCountWrites() {
        return zoneCountWrites;
    }

    public StoreDefinitionBuilder setZoneCountWrites(Integer zoneCountWrites) {
        this.zoneCountWrites = zoneCountWrites;
        return this;
    }

    public HintedHandoffStrategyType getHintedHandoffStrategy() {
        return hintedHandoffStrategy;
    }

    public StoreDefinitionBuilder setHintedHandoffStrategy(HintedHandoffStrategyType hintedHandoffStrategy) {
        this.hintedHandoffStrategy = hintedHandoffStrategy;
        return this;
    }

    public Integer getHintPrefListSize() {
        return hintPrefListSize;
    }

    public StoreDefinitionBuilder setHintPrefListSize(Integer hintPrefListSize) {
        this.hintPrefListSize = hintPrefListSize;
        return this;
    }

    public StoreDefinition build() {
        return new StoreDefinition(this.getName(),
                                   this.getType(),
                                   this.getKeySerializer(),
                                   this.getValueSerializer(),
                                   this.getTransformsSerializer(),
                                   this.getRoutingPolicy(),
                                   this.getRoutingStrategyType(),
                                   this.getReplicationFactor(),
                                   this.getPreferredReads(),
                                   this.getRequiredReads(),
                                   this.getPreferredWrites(),
                                   this.getRequiredWrites(),
                                   this.getViewOf(),
                                   this.getView(),
                                   this.getZoneReplicationFactor(),
                                   this.getZoneCountReads(),
                                   this.getZoneCountWrites(),
                                   this.getRetentionPeriodDays(),
                                   this.getRetentionScanThrottleRate(),
                                   this.getSerializerFactory(),
                                   this.getHintedHandoffStrategy(),
                                   this.getHintPrefListSize(),
                                   this.getProperties());
    }
}
