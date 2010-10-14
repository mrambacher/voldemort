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

package voldemort.store.routed;

import java.io.Serializable;

import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * A wrapper around a node id, key and value. This class represents one
 * key/value as fetched from a single server.
 * 
 * 
 * @param <K> The type of the key
 * @param <V> The type of the value
 * 
 */
final public class NodeValue<N, K, V> implements Serializable, Cloneable {

    private static final long serialVersionUID = 1;

    private final N node;
    private final K key;
    private final Versioned<V> value;

    public NodeValue(N node, K key, Versioned<V> value) {
        this.node = node;
        this.key = Preconditions.checkNotNull(key);
        this.value = Preconditions.checkNotNull(value);
    }

    public N getNode() {
        return node;
    }

    public K getKey() {
        return key;
    }

    public Versioned<V> getVersioned() {
        return value;
    }

    public Version getVersion() {
        return value.getVersion();
    }

    @Override
    public NodeValue<N, K, V> clone() {
        return new NodeValue<N, K, V>(node, key, value);
    }

    @Override
    public String toString() {
        return "NodeValue(id=" + node + ", key=" + key + ", versioned= " + value + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(node, key, value.getVersion());
    }

    @Override
    public boolean equals(Object o) {
        if(o == this)
            return true;
        if(!(o instanceof NodeValue<?, ?, ?>))
            return false;

        NodeValue<?, ?, ?> v = (NodeValue<?, ?, ?>) o;
        return Objects.equal(getNode(), v.getNode()) && Objects.equal(getKey(), v.getKey())
               && Objects.equal(getVersion(), v.getVersion());
    }
}
