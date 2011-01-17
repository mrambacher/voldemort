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

package voldemort.client.protocol;

import voldemort.utils.ByteUtils;

public enum RequestFormatType {
    VOLDEMORT_V0("vp", "voldemort-native-v0", 0),
    VOLDEMORT_V1("vp", "voldemort-native-v1", 1),
    VOLDEMORT_V2("vp", "voldemort-native-v2", 2),
    VOLDEMORT_V3("vp", "voldemort-native-v3", 3),
    VOLDEMORT_V4("vp", "voldemort-native-v4", 4),
    PROTOCOL_BUFFERS("pb", "protocol-buffers-v0", 0),
    ADMIN_PROTOCOL_BUFFERS("ad", "admin-v1", 1);

    private final String protocol;
    private final String displayName;
    private final int version;

    private RequestFormatType(String protocol, String display, int version) {
        this.protocol = protocol;
        this.displayName = display;
        this.version = version;
    }

    public String getCode() {
        return protocol + version;
    }

    /**
     * Returns the protocol code for this type
     * 
     * @return The protocol code
     */
    public String getProtocol() {
        return protocol;
    }

    public int getVersion() {
        return version;
    }

    /**
     * Returns the request version as a 2-character string
     * 
     * @return The string representation of the version for this type.
     */
    public String getVersionAsString() {
        return String.format("%02d", version);
    }

    public String getDisplayName() {
        return this.displayName;
    }

    /**
     * Converts the input argument into the "closest match" RequestFormatType.
     * This method looks for a RequestFormatType that has the same protocol and
     * version. If one is found, it is returned. If the protocol is found but
     * not the version, the highest lesser existing version is returned (e.g.,
     * version 3 if 4 is requested), or, barring lesser versions, the lowest
     * greater version (e.g., 5 if 4 was requested) is returned. If the protocol
     * is not found, an exception is thrown.
     * 
     * @param protocol The protocol to search for
     * @param version The preferred version of the protocol to match
     * @return The best-matching RequestFormatType, if one is found
     * @throws IllegalArgumentException if the protocol is not supported
     */
    public static RequestFormatType fromCode(String protocol, int version) {
        RequestFormatType match = null;
        for(RequestFormatType type: RequestFormatType.values()) { // For each
            // Type
            if(type.getProtocol().equals(protocol)) { // Do the protocols match?
                if(type.getVersion() == version) { // Do the versions match?
                    return type; // Yes, return requested protocol
                } else if(match == null) { // Any others for this protocol?
                    match = type; // No, store this one
                } else if(version > type.getVersion()) { // If the requested
                    // version is greater
                    if(type.getVersion() > match.getVersion()) { // Current is
                        // closer than
                        // match
                        match = type; // Update the match
                    }
                } else { // Requested version is less
                    if(version < match.getVersion() && // Match less than
                       // requested
                       type.getVersion() < match.getVersion()) { // And current
                        // less than
                        // match
                        match = type; // Update the match
                    }
                }
            }
        }
        if(match != null) {
            return match;
        } else {
            throw new IllegalArgumentException("Requested protocol '" + protocol + "' not found");
        }
    }

    /**
     * Returns the best-matching format type from the input byte array. The
     * first two bytes are the UTF-8 representation of the protocol (e.g. "vp").
     * The third byte is the character representation of the version (e.g. "3").
     * 
     * @param bytes The array of UTF-8 encoded characters for this type (e.g.
     *        "vp3").
     * @return The closest-matching request type for the input bytes
     * @throws IllegalArgumentException if the input bytes is not the correct
     *         length
     * @throws IllegalArgumentException if the protocol is not supported
     */
    public static RequestFormatType fromBytes(byte[] bytes) {
        if(bytes.length != 3) {
            String code = ByteUtils.getString(bytes, "UTF-8");
            throw new IllegalArgumentException("No request format '" + code + "' was found");
        }
        byte[] protoBytes = { bytes[0], bytes[1] };
        byte[] versionBytes = { bytes[2] };

        String proto = ByteUtils.getString(protoBytes, "UTF-8");
        int version = Integer.parseInt(ByteUtils.getString(versionBytes, "UTF-8"));
        return RequestFormatType.fromCode(proto, version);
    }

    /**
     * Returns the exact-matching format type from the input code.
     * 
     * @param code The code to match to an existing format type.
     * @return The matching request type for the code
     * @throws IllegalArgumentException if the code was not found
     */
    public static RequestFormatType fromCode(String code) {
        for(RequestFormatType type: RequestFormatType.values()) {
            if(type.getCode().equals(code)) {
                return type;
            }
        }
        throw new IllegalArgumentException("No request format '" + code + "' was found");
    }

}
