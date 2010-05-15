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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
            int numEntries = ByteUtils.readShort(bytes, offset);
            int versionSize = bytes[offset + 2];
            if(numEntries < 0 && versionSize == 0) {
                return isValidProtocol(bytes, offset + 3);
            } else {
                return false;
            }
        }
    }

    public static boolean isValidProtocol(byte[] bytes, int offset) {
        if(bytes == null || bytes.length <= 0) {
            return false;
        } else {
            try {
                CodedInputStream in = CodedInputStream.newInstance(bytes, offset, bytes.length);
                String protocol = in.readString();
                return PROTOCOL.equals(protocol);
            } catch(IOException e) { /* Should not happen */
                return false;
            }
        }
    }

    public static int sizeInBytes(Metadata md) {
        Set<String> props = md.listProperties();
        int size = CodedOutputStream.computeStringSizeNoTag(PROTOCOL);
        size += CodedOutputStream.computeInt64SizeNoTag(props.size());
        for(String p: props) {
            size += CodedOutputStream.computeStringSizeNoTag(p);
            size += CodedOutputStream.computeStringSizeNoTag(md.getProperty(p));
        }
        return size;
    }

    public static byte[] toBytes(Metadata md) {
        byte[] bytes = new byte[sizeInBytes(md)];
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        try {
            out.writeStringNoTag(PROTOCOL);
            Set<String> props = md.listProperties();
            out.writeInt32NoTag(props.size());
            for(String p: props) {
                out.writeStringNoTag(p);
                out.writeStringNoTag(md.getProperty(p));
            }
        } catch(IOException e) { /* Should not happen */
            throw new RuntimeException(e);
        }
        return bytes;
    }

    public static boolean isValidMetadata(byte[] bytes) {
        return isValidMetadata(bytes, 0);
    }

    public static boolean isValidMetadata(byte[] bytes, int offset) {
        return isValidProtocol(bytes, offset);
    }

    public static Metadata toMetadata(byte[] bytes) {
        return toMetadata(bytes, 0);
    }

    public static Metadata toMetadata(byte[] bytes, int offset) {
        if(!isValidMetadata(bytes, offset)) {
            throw new IllegalArgumentException("Metadata encoded incorrectly for " + PROTOCOL);
        } else {
            CodedInputStream in = CodedInputStream.newInstance(bytes, offset, bytes.length);
            try {
                String protocol = in.readString();
                if(!PROTOCOL.equals(protocol)) {
                    throw new IllegalArgumentException("Wrong protocol " + PROTOCOL);
                }
                Metadata md = new Metadata();
                int numEntries = in.readInt32();
                for(int i = 0; i < numEntries; i++) {
                    String name = in.readString();
                    String value = in.readString();
                    md.setProperty(name, value);
                }
                return md;
            } catch(IOException e) { /* Should not happen */
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> byte[] toBytes(Versioned<T> versioned, Serializer<T> serializer) {
        byte[] version = toBytes(versioned.getVersion());
        byte[] metadata = toBytes(versioned.getMetadata());
        byte[] data = serializer.toBytes(versioned.getValue());
        return ByteUtils.cat(version, metadata, data);
    }

    public static int sizeInBytes(Versioned<byte[]> versioned) {
        byte[] value = versioned.getValue();
        int size = sizeInBytes(versioned.getVersion()) + sizeInBytes(versioned.getMetadata());
        if(value != null) {
            size += value.length;
        }
        return size;
    }

    public static byte[] toBytes(Versioned<byte[]> versioned) {
        byte[] version = toBytes(versioned.getVersion());
        byte[] metadata = toBytes(versioned.getMetadata());
        return ByteUtils.cat(version, metadata, versioned.getValue());
    }

    public static Versioned<byte[]> toVersioned(byte[] data) {
        return toVersioned(data, 0);
    }

    public static Versioned<byte[]> toVersioned(byte[] data, int offset) {
        Version version = VectorClockProtoSerializer.toVersion(data, offset);
        int size = VectorClockProtoSerializer.sizeInBytes(version);
        Metadata md = VectorClockProtoSerializer.toMetadata(data, offset + size);
        size += md.sizeInBytes();
        return new Versioned<byte[]>(ByteUtils.copy(data, offset + size, data.length), version, md);
    }
}
