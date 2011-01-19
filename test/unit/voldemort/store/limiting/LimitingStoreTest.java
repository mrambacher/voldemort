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

import java.io.StringReader;

import org.junit.Test;

import voldemort.VoldemortTestConstants;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class LimitingStoreTest extends AbstractByteArrayStoreTest {

    public LimitingStoreTest() {
        super("test");
    }

    @Override
    public Store<ByteArray, byte[], byte[]> createStore(String name) {
        Store<ByteArray, byte[], byte[]> store = new InMemoryStore<ByteArray, byte[], byte[]>(name);
        return new LimitingStore(store, 300, 30 * 1024 * 1024, 200);
    }

    @Test
    @Override
    public void testSixtyMegabyteSizes() {
        testValueSizes("250-byte keys and with value size = 60*1024*1024 bytes (60 MB).",
                       60 * 1024 * 1024,
                       250,
                       LimitExceededException.class);
    }

    @Test
    public void testLargeKeySize() {
        testValueSizes("1-K keys and with value size = 1024 bytes.",
                       1024,
                       1024,
                       LimitExceededException.class);
    }

    @Test
    public void testLargeMetadata() {
        String key = "123";
        byte[] data = key.getBytes();
        Versioned<byte[]> value = new Versioned<byte[]>(data);
        try {
            for(int i = 0; i < 100; i++) {
                String name = Integer.toString(i);
                value.getMetadata().setProperty(name, name);
            }
            Store<ByteArray, byte[], byte[]> store = getStore();
            store.put(new ByteArray(data), value, null);
            fail("Expected metadata size limit exceeded");
        } catch(Exception e) {
            assertEquals("Unexpected exception", LimitExceededException.class, e.getClass());
        }
    }

    @Test
    public void testStoreDefinition() {
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        StoreDefinition storeDef = mapper.readStoreList(new StringReader(VoldemortTestConstants.getStoreWithPropertiesXml()))
                                         .get(0);
        Store<ByteArray, byte[], byte[]> store = new InMemoryStore<ByteArray, byte[], byte[]>(storeDef.getName());
        LimitingStore limit = new LimitingStore(store, storeDef);
        assertTrue("Key limit (" + limit.maxKeySize + "> 0", (limit.maxKeySize > 0));
        assertTrue("Value limit (" + limit.maxValueSize + "> 0", (limit.maxValueSize > 0));
        assertTrue("Metadata limit (" + limit.maxMetadataSize + "> 0", (limit.maxMetadataSize > 0));
    }
}
