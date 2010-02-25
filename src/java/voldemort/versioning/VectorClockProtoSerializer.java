/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright © 2010 Nokia Corporation. All rights reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import voldemort.serialization.Serializer;
import voldemort.utils.ByteUtils;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

public class VectorClockProtoSerializer {

    public static String PROTOCOL = "vc1";

    public static int sizeInBytes(Version version) {
        VectorClock clock = (VectorClock) version;
        List<ClockEntry> versions = clock.getEntries();
        long timestamp = clock.getTimestamp();
        int size = CodedOutputStream.computeStringSizeNoTag(PROTOCOL);

        size += CodedOutputStream.computeInt64SizeNoTag(timestamp);
        size += CodedOutputStream.computeInt32SizeNoTag(versions.size());
        for(ClockEntry v: versions) {
            size += CodedOutputStream.computeInt32SizeNoTag(v.getNodeId());
            size += CodedOutputStream.computeInt64SizeNoTag(v.getVersion());
        }
        return size + ByteUtils.SIZE_OF_SHORT + 1;
    }

    public static byte[] toBytes(Version version) {
        VectorClock clock = (VectorClock) version;

        byte[] serialized = new byte[sizeInBytes(version)];
        ByteUtils.writeShort(serialized, (short) -1, 0);
        serialized[2] = 0;

        List<ClockEntry> versions = clock.getEntries();
        long timestamp = clock.getTimestamp();
        CodedOutputStream out = CodedOutputStream.newInstance(serialized, 3, serialized.length);
        try {
            out.writeStringNoTag(PROTOCOL);
            out.writeInt64NoTag(timestamp);
            out.writeInt32NoTag(versions.size());
            for(ClockEntry v: versions) {
                out.writeInt32NoTag(v.getNodeId());
                out.writeInt64NoTag(v.getVersion());
            }
        } catch(IOException e) { /* Should not happen */
            throw new RuntimeException(e);
        }
        return serialized;
    }

    public static Version toVersion(byte[] bytes) {
        return toVersion(bytes, 0);
    }

    public static <T> byte[] toBytes(Versioned<T> versioned, Serializer<T> serializer) {
        byte[] version = toBytes(versioned.getVersion());
        byte[] data = serializer.toBytes(versioned.getValue());
        return ByteUtils.cat(version, data);
    }

    public static byte[] toBytes(Versioned<byte[]> versioned) {
        byte[] version = toBytes(versioned.getVersion());
        return ByteUtils.cat(version, versioned.getValue());
    }

    public static Version toVersion(byte[] bytes, int offset) {
        if(!isValid(bytes, offset)) {
            throw new IllegalArgumentException("Clock encoded incorrectly for " + PROTOCOL);
        } else {
            CodedInputStream in = CodedInputStream.newInstance(bytes, offset + 3, bytes.length);
            try {
                String protocol = in.readString();
                if(!PROTOCOL.equals(protocol)) {
                    throw new IllegalArgumentException("Wrong protocol " + PROTOCOL);
                }
                long timestamp = in.readInt64();
                int numEntries = in.readInt32();
                List<ClockEntry> entries = new ArrayList<ClockEntry>(numEntries);
                for(int i = 0; i < numEntries; i++) {
                    short nodeId = (short) in.readInt32();
                    long version = in.readInt64();
                    entries.add(new ClockEntry(nodeId, version));
                }
                return new VectorClock(entries, timestamp);
            } catch(IOException e) { /* Should not happen */
                throw new RuntimeException(e);
            }
        }
    }

    public static boolean isValid(byte[] bytes) {
        return isValid(bytes, 0);
    }

    public static boolean isValid(byte[] bytes, int offset) {
        if(bytes == null || bytes.length < offset + 3) {
            return false;
        } else {
            try {
                int numEntries = ByteUtils.readShort(bytes, offset);
                int versionSize = bytes[offset + 2];
                if(numEntries < 0 && versionSize == 0) {
                    CodedInputStream in = CodedInputStream.newInstance(bytes,
                                                                       offset + 3,
                                                                       bytes.length);
                    String protocol = in.readString();
                    return PROTOCOL.equals(protocol);
                } else {
                    return false;
                }
            } catch(IOException e) { /* Should not happen */
                return false;
            }
        }
    }
}
