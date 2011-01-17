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

package voldemort.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.utils.ByteUtils;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

public final class VoldemortOperation {

    private final VoldemortOpCode operation;
    private final String key;
    private final byte[] value;
    private final Version version;

    private VoldemortOperation(VoldemortOpCode operation, String key, byte[] value, Version version) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public VoldemortOperation(byte[] bytes) {
        if(bytes == null || bytes.length <= 1)
            throw new SerializationException("Not enough bytes to serialize");
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            byte opCode = inputStream.readByte();
            operation = VoldemortOpCode.fromCode(opCode);
            switch(operation) {
                case GET:
                    this.version = null;
                    this.key = inputStream.readUTF();
                    this.value = null;
                    break;
                case PUT:
                    this.version = VersionFactory.toVersion(bytes, 1);
                    this.key = inputStream.readUTF();
                    int valueSize = inputStream.readInt();
                    this.value = new byte[valueSize];
                    ByteUtils.read(inputStream, this.value);
                    break;
                case DELETE:
                    this.version = VersionFactory.toVersion(bytes, 1);
                    this.key = inputStream.readUTF();
                    this.value = null;
                    break;
                default:
                    throw new SerializationException("Unsupported operation: " + operation);
            }
        } catch(IllegalArgumentException e) {
            throw new SerializationException(e.getMessage(), e);
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteOutput);
            output.writeByte(operation.asCode());
            if(operation != VoldemortOpCode.GET)
                output.write(version.toBytes());
            output.writeUTF(key);
            if(operation == VoldemortOpCode.PUT) {
                output.writeInt(value.length);
                output.write(value);
            }
            return byteOutput.toByteArray();
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public VoldemortOperation makeGetOperation(String key) {
        return new VoldemortOperation(VoldemortOpCode.GET, key, null, null);
    }

    public VoldemortOperation makePutOperation(String key, Versioned<byte[]> versioned) {
        return new VoldemortOperation(VoldemortOpCode.PUT,
                                      key,
                                      versioned.getValue(),
                                      versioned.getVersion());
    }

    public VoldemortOperation makeDeleteOperation(String key, Version version) {
        return new VoldemortOperation(VoldemortOpCode.DELETE, key, null, version);
    }

}
