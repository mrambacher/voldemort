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

package voldemort.protocol;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.client.protocol.RequestFormatType;

public class RequestFormatTypeTest extends TestCase {

    @Test
    public void testFromCode() {
        for(RequestFormatType type: RequestFormatType.values()) {
            assertEquals("Type matches code", type, RequestFormatType.fromCode(type.getCode()));
        }
    }

    @Test
    public void testFromCodeWithVersion() {
        for(RequestFormatType type: RequestFormatType.values()) {
            assertEquals("Type matches code", type, RequestFormatType.fromCode(type.getProtocol(),
                                                                               type.getVersion()));
        }
    }

    @Test
    public void testNonexistentCodes() {
        try {
            RequestFormatType type = RequestFormatType.fromCode("ab2");
            fail("Unexpected type returned " + type);
        } catch(Exception e) {
            assertEquals("Expected IllegalArgument", IllegalArgumentException.class, e.getClass());
        }
        try {
            RequestFormatType type = RequestFormatType.fromCode("ab", 2);
            fail("Unexpected type returned " + type);
        } catch(Exception e) {
            assertEquals("Expected IllegalArgument", IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void testClosestMatch() {
        assertEquals("Returned lowest matching version",
                     RequestFormatType.VOLDEMORT_V0,
                     RequestFormatType.fromCode("vp", -1));
        assertEquals("Returned highest matching version",
                     RequestFormatType.VOLDEMORT_V3,
                     RequestFormatType.fromCode("vp", 10));
        assertEquals("Returned lowest matching version",
                     RequestFormatType.PROTOCOL_BUFFERS,
                     RequestFormatType.fromCode("pb", -1));
        assertEquals("Returned highest matching version",
                     RequestFormatType.PROTOCOL_BUFFERS,
                     RequestFormatType.fromCode("pb", 10));
        assertEquals("Returned lowest matching version",
                     RequestFormatType.ADMIN_PROTOCOL_BUFFERS,
                     RequestFormatType.fromCode("ad", -1));
        assertEquals("Returned highest matching version",
                     RequestFormatType.ADMIN_PROTOCOL_BUFFERS,
                     RequestFormatType.fromCode("ad", 10));
    }
}
