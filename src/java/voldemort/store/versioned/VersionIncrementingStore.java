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

package voldemort.store.versioned;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A wrapper that increments the version on the value for puts and delegates all
 * other operations
 * 
 * @author jay
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
public class VersionIncrementingStore<K, V> extends DelegatingStore<K, V> implements Store<K, V> {

    private final short nodeId;
    private final Time time;

    public VersionIncrementingStore(Store<K, V> innerStore, int nodeId, Time time) {
        super(innerStore);
        this.nodeId = (short) nodeId;
        this.time = time;
    }

    @Override
    public Version put(K key, Versioned<V> value) throws VoldemortException {

        value = value.cloneVersioned();
        Version clock = value.getVersion();
        clock.incrementClock(nodeId, time.getMilliseconds());
        return super.put(key, value);
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(StoreCapabilityType.VERSION_INCREMENTING.equals(capability))
            return true;
        else
            return super.getCapability(capability);
    }

}
