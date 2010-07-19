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
package voldemort.store;

import java.util.HashMap;
import java.util.Map;

import voldemort.server.VoldemortConfig;
import voldemort.utils.ByteArray;

public class DoNothingStorageConfiguration implements StorageConfiguration {

    private Map<String, StorageEngine<ByteArray, byte[]>> stores;

    @SuppressWarnings("unused")
    public DoNothingStorageConfiguration(VoldemortConfig config) {
        stores = new HashMap<String, StorageEngine<ByteArray, byte[]>>();
    }

    public String getType() {
        return "null";
    }

    public void close() {
        for(StorageEngine<ByteArray, byte[]> store: stores.values()) {
            store.close();
        }
        stores.clear();
    }

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        StorageEngine<ByteArray, byte[]> store = stores.get(name);
        if(store == null) {
            store = new DoNothingStore<ByteArray, byte[]>(name);
            stores.put(name, store);
        }
        return store;
    }
}
