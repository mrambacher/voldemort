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

import java.util.ArrayList;
import java.util.List;

import voldemort.serialization.Serializer;
import voldemort.utils.ByteUtils;

public class VectorClockVersionSerializer {

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
     * Read the vector clock from the given bytes starting from a particular
     * offset
     * 
     * @param bytes The bytes to read from
     * @param offset The offset to start reading from
     */
    public static Version toVersion(byte[] bytes, int offset) {
        if(bytes == null || bytes.length <= offset) {
            throw new IllegalArgumentException("Invalid byte array for serialization--no bytes to read.");
        }
        int numEntries = ByteUtils.readShort(bytes, offset);
        int versionSize = bytes[offset + 2];
        int entrySize = ByteUtils.SIZE_OF_SHORT + versionSize;
        int minimumBytes = offset + ByteUtils.SIZE_OF_SHORT + 1 + numEntries * entrySize
                           + ByteUtils.SIZE_OF_LONG;
        if(bytes.length < minimumBytes)
            throw new IllegalArgumentException("Too few bytes: expected at least " + minimumBytes
                                               + " but found only " + bytes.length + ".");

        List<ClockEntry> entries = new ArrayList<ClockEntry>(numEntries);
        int index = 3 + offset;
        for(int i = 0; i < numEntries; i++) {
            short nodeId = ByteUtils.readShort(bytes, index);
            long version = ByteUtils.readBytes(bytes, index + ByteUtils.SIZE_OF_SHORT, versionSize);
            entries.add(new ClockEntry(nodeId, version));
            index += entrySize;
        }
        long timestamp = ByteUtils.readLong(bytes, index);
        return new VectorClock(entries, timestamp);
    }

    public static byte[] toBytes(Version version) {
        VectorClock clock = (VectorClock) version;

        byte[] serialized = new byte[sizeInBytes(version)];
        // write the number of versions
        List<ClockEntry> entries = clock.getEntries();
        ByteUtils.writeShort(serialized, (short) entries.size(), 0);
        // write the size of each version in bytes
        byte versionSize = ByteUtils.numberOfBytesRequired(clock.getMaxVersion());
        serialized[2] = versionSize;

        int clockEntrySize = ByteUtils.SIZE_OF_SHORT + versionSize;
        int start = 3;
        for(ClockEntry v: entries) {
            ByteUtils.writeShort(serialized, v.getNodeId(), start);
            ByteUtils.writeBytes(serialized,
                                 v.getVersion(),
                                 start + ByteUtils.SIZE_OF_SHORT,
                                 versionSize);
            start += clockEntrySize;
        }
        ByteUtils.writeLong(serialized, clock.getTimestamp(), start);
        return serialized;
    }

    public static int sizeInBytes(Version version) {
        VectorClock clock = (VectorClock) version;
        byte versionSize = ByteUtils.numberOfBytesRequired(clock.getMaxVersion());
        return ByteUtils.SIZE_OF_SHORT + 1 + clock.getEntries().size()
               * (ByteUtils.SIZE_OF_SHORT + versionSize) + ByteUtils.SIZE_OF_LONG;
    }

    public static boolean isValid(byte[] bytes) {
        return isValid(bytes, 0);
    }

    public static boolean isValid(byte[] bytes, int offset) {
        int minimumBytes = offset + ByteUtils.SIZE_OF_SHORT + ByteUtils.SIZE_OF_LONG;
        boolean isValid = true;
        if(bytes == null || bytes.length <= minimumBytes) {
            isValid = false;
        } else {
            int numEntries = ByteUtils.readShort(bytes, offset);
            int versionSize = bytes[offset + 2];
            if(numEntries < 0 && versionSize == 0) {
                isValid = false;
            } else {
                int entrySize = ByteUtils.SIZE_OF_SHORT + versionSize;
                minimumBytes += (numEntries * entrySize);
                if(bytes.length < minimumBytes) {
                    isValid = false;
                }
            }
        }
        return isValid;
    }

    public static <T> byte[] toBytes(Versioned<T> versioned, Serializer<T> serializer) {
        byte[] version = toBytes(versioned.getVersion());
        byte[] data = serializer.toBytes(versioned.getValue());
        return ByteUtils.cat(version, data);
    }

    public static int sizeInBytes(Versioned<byte[]> versioned) {
        return sizeInBytes(versioned.getVersion()) + versioned.getValue().length;
    }

    public static byte[] toBytes(Versioned<byte[]> versioned) {
        byte[] version = toBytes(versioned.getVersion());
        return ByteUtils.cat(version, versioned.getValue());
    }

    public static Versioned<byte[]> toVersioned(byte[] data) {
        return toVersioned(data, 0);
    }

    public static Versioned<byte[]> toVersioned(byte[] data, int offset) {
        Version version = VectorClockVersionSerializer.toVersion(data, offset);
        int size = VectorClockVersionSerializer.sizeInBytes(version);
        return new Versioned<byte[]>(ByteUtils.copy(data, offset + size, data.length), version);
    }
}