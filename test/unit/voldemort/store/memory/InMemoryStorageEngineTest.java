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

package voldemort.store.memory;

import java.util.ArrayList;
import java.util.List;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

public class InMemoryStorageEngineTest extends AbstractStorageEngineTest {

    public InMemoryStorageEngineTest() {
        super("test", InMemoryStorageConfiguration.TYPE_NAME);
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> createStorageEngine(StoreDefinition def) {
        return new InMemoryStorageEngine(def);
    }

    @Override
    public List<ByteArray> getKeys(int numKeys) {
        List<ByteArray> keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(new ByteArray(TestUtils.randomBytes(10)));
        return keys;
    }

}
