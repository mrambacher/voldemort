/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright 2010 Nokia Corporation. All rights reserved.
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
package voldemort.versioning;

import voldemort.serialization.Serializer;

public class VersionFactory {

    /**
     * Returns a new version object
     * 
     * @return The new (empty) version
     */
    public static Version newVersion() {
        return new VectorClock();
    }

    /**
     * Takes the bytes of a VectorClock and creates a java object from them. For
     * efficiency reasons the extra bytes can be attached to the end of the byte
     * array that are not related to the VectorClock
     * 
     * @param bytes The serialized bytes of the VectorClock
     */
    public static Version toVersion(byte[] bytes) {
        return toVersion(bytes, 0);
    }

    /**
     * Read the version from the given bytes starting from a particular offset
     * 
     * @param bytes The bytes to read from
     * @param offset The offset to start reading from
     */
    public static Version toVersion(byte[] bytes, int offset) {
        if(VectorClockVersionSerializer.isValid(bytes, offset)) {
            return VectorClockVersionSerializer.toVersion(bytes, offset);
        } else if(VectorClockProtoSerializer.isValid(bytes, offset)) {
            return VectorClockProtoSerializer.toVersion(bytes, offset);
        } else {
            throw new IllegalArgumentException("Unrecognized version encoding");
        }
    }

    /**
     * Takes the bytes of Metadata and creates a java object from them. For
     * efficiency reasons the extra bytes can be attached to the end of the byte
     * array that are not related to the Metadata
     * 
     * @param bytes The serialized bytes of the Metadata
     */
    public static Metadata toMetadata(byte[] bytes) {
        return toMetadata(bytes, 0);
    }

    /**
     * Read the Metadata from the given bytes starting from a particular offset
     * 
     * @param bytes The bytes to read from
     * @param offset The offset to start reading from
     */
    public static Metadata toMetadata(byte[] bytes, int offset) {
        if(bytes == null || bytes.length <= offset) {
            return new Metadata();
        } else if(VectorClockProtoSerializer.isValidMetadata(bytes, offset)) {
            return VectorClockProtoSerializer.toMetadata(bytes, offset);
        } else {
            throw new IllegalArgumentException("Unrecognized version encoding");
        }
    }

    /**
     * Read the versioned byte object from the given bytes starting from a
     * particular offset
     * 
     * @param bytes The bytes to read from
     * @return The reconstructed object
     */
    public static Versioned<byte[]> toVersioned(byte[] data) {
        return toVersioned(data, 0);
    }

    /**
     * Read the versioned byte object from the given bytes starting from a
     * particular offset
     * 
     * @param bytes The bytes to read from
     * @param offset The offset to start reading from
     * @return The reconstructed object
     */
    public static Versioned<byte[]> toVersioned(byte[] data, int offset) {
        if(VectorClockVersionSerializer.isValid(data, offset)) {
            return VectorClockVersionSerializer.toVersioned(data, offset);
        } else if(VectorClockProtoSerializer.isValid(data, offset)) {
            return VectorClockProtoSerializer.toVersioned(data, offset);
        } else {
            throw new IllegalArgumentException("Unrecognized version encoding");
        }
    }

    /**
     * Read the versioned object from the given bytes
     * 
     * @param bytes The bytes to read from
     * @param serializer the serializer to convert the input bytes to object T
     * @return The reconstructed object
     */
    public static <T> Versioned<T> toVersioned(byte[] data, Serializer<T> serializer) {
        Versioned<byte[]> versioned = toVersioned(data);
        T object = serializer.toObject(versioned.getValue());
        return new Versioned<T>(object, versioned.getVersion(), versioned.getMetadata());
    }

    /**
     * Read the versioned object from the given bytes
     * 
     * @param bytes The bytes to read from
     * @param serializer the serializer to convert the input bytes to object T
     * @return The reconstructed object
     */
    public static <T> byte[] toBytes(Versioned<T> versioned, Serializer<T> serializer) {
        byte[] data = serializer.toBytes(versioned.getValue());
        Versioned<byte[]> value = new Versioned<byte[]>(data,
                                                        versioned.getVersion(),
                                                        versioned.getMetadata());
        return VectorClockProtoSerializer.toBytes(value);
    }

    /**
     * Creates a copy of the input version
     * 
     * @param version The version being copied
     * @return The deep copy of the version
     */
    public static Version cloneVersion(Version version) {
        VectorClock clock = (VectorClock) version;
        return clock.clone();
    }

    /**
     * Returns a new version object with the clock incremented for the input
     * node
     * 
     * @param nodeId The node to increment
     * @param timestamp The timestamp for the version
     * 
     * @return The copied version with the new clock
     */
    public static Version incremented(Version version, int nodeId, long timestamp) {
        Version incremented = cloneVersion(version);
        incremented.incrementClock(nodeId, timestamp);
        return incremented;
    }

    /**
     * Returns a new versioned object with the clock incremented for the input
     * node
     * 
     * @param nodeId The node to increment
     * @param timestamp The timestamp for the version
     * 
     * @return The copied version with the new clock
     */
    public static <T> Versioned<T> incremented(Versioned<T> versioned, int nodeId, long timestamp) {
        Version incremented = incremented(versioned.getVersion(), nodeId, timestamp);
        return new Versioned<T>(versioned.getValue(), incremented);
    }
}
