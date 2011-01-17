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

package voldemort.serialization;

/**
 * An enumeration representing the messages between a client and server. Each
 * request has an encoding (byte) and an associated name (String)
 */
public enum VoldemortOpCode {
    GET("GET", 1),
    PUT("PUT", 2),
    DELETE("DELETE", 3),
    GET_ALL("GET_ALL", 4),
    GET_PARTITION_AS_STREAM("GET_PARTITION", 5),
    PUT_ENTRIES_AS_STREAM("PUT_ENTRIES", 6),
    DELETE_PARTITIONS("DELETE_PARTITIONS", 7),
    UPDATE_METADATA("UPDATE_METADATA", 8),
    REDIRECT_GET("REDIRECT_GET", 9),
    GET_VERSION("GET_VERSION", 10),
    GET_METADATA("GET_METADATA", 11);

    private final String method;
    private final int code;

    private VoldemortOpCode(String method, int code) {
        this.method = method;
        this.code = code;
    }

    @Override
    public String toString() {
        return method;
    }

    public byte asCode() {
        return (byte) code;
    }

    public String getMethodName() {
        return method;
    }

    /**
     * Returns the code for the byte value
     * 
     * @param code The byte representing the request
     * @return The matching OpCode or an exception if one cannot be found
     */
    public static VoldemortOpCode fromCode(byte code) {
        for(VoldemortOpCode operation: VoldemortOpCode.values()) {
            if(code == operation.asCode()) {
                return operation;
            }
        }
        throw new IllegalArgumentException("Requested operation '" + code + "' not found");
    }
}
