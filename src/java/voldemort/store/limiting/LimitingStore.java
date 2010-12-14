/*
 * Copyright 2010 Nokia Corporation. All rights reserved.
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

package voldemort.store.limiting;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store wrapper that limits the sizes of keys and/or values
 * 
 * @author mark
 * 
 */
public class LimitingStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    public static final String MAX_KEY_SIZE_PROPERTY = "max.key.size";
    public static final String MAX_VALUE_SIZE_PROPERTY = "max.value.size";
    public static final String MAX_METADATA_SIZE_PROPERTY = "max.metadata.size";
    final int maxKeySize;
    final int maxMetadataSize;
    final int maxValueSize;

    public LimitingStore(Store<ByteArray, byte[], byte[]> store, StoreDefinition storeDef) {
        super(store);
        maxKeySize = storeDef.getIntProperty(MAX_KEY_SIZE_PROPERTY, 0);
        maxValueSize = storeDef.getIntProperty(MAX_VALUE_SIZE_PROPERTY, 0);
        maxMetadataSize = storeDef.getIntProperty(MAX_METADATA_SIZE_PROPERTY, 0);
    }

    /**
     * Create a limiting store that wraps the given store
     * 
     * @param store The store to wrap
     * @param keyLimit The limit to the size of the key (<= 0 means unlimited)
     * @param valueLimit The limit to the size of the value (<= 0 means
     *        unlimited)
     */
    public LimitingStore(Store<ByteArray, byte[], byte[]> store,
                         int keyLimit,
                         int valueLimit,
                         int metadataLimit) {
        super(store);
        this.maxKeySize = keyLimit;
        this.maxValueSize = valueLimit;
        this.maxMetadataSize = metadataLimit;
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        checkKeyValidity(key);
        return super.delete(key, version);
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        checkKeyValidity(key);
        return super.get(key, transforms);
    }

    @Override
    public Version put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        checkKeyValidity(key);
        checkValueValidity(value);
        return super.put(key, value, transforms);
    }

    private void checkKeyValidity(ByteArray key) throws VoldemortException {
        if(maxKeySize > 0) {
            if(key != null && key.length() > maxKeySize) {
                throw new LimitExceededException("Key is too large max=" + maxKeySize + "<"
                                                 + key.length());
            }
        }
    }

    private void checkValueValidity(Versioned<byte[]> versioned) throws VoldemortException {
        if(maxValueSize > 0) {
            byte[] value = versioned.getValue();
            if(value != null && value.length > maxValueSize) {
                throw new LimitExceededException("Value is too large max=" + maxValueSize + "<"
                                                 + value.length);
            }
        }
        if(maxMetadataSize > 0) {
            byte[] metadata = versioned.getMetadata().toBytes();
            if(metadata != null && metadata.length > maxMetadataSize) {
                throw new LimitExceededException("Metadata is too large max=" + maxMetadataSize
                                                 + "<" + metadata.length);

            }
        }
    }
}
