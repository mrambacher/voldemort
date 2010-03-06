/**
 * See the NOTICE.txt file distributed with this work for information regarding
 * copyright ownership.
 * 
 * The authors license this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.store.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.mongodb.driver.MongoDBException;
import org.mongodb.driver.ts.Doc;

import voldemort.VoldemortException;
import voldemort.serialization.mongodb.MongoDBDocSerializer;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

/**
 * Tests for MongoDBStorageEngine. Copied tests from StorageEngineTest as they
 * made assumptions about key and value types rather than defer to getValues()
 * and getKeys()
 * 
 */
public class MongoDBStorageEngineTest extends AbstractStorageEngineTest {

    MongoDBDocSerializer mds = new MongoDBDocSerializer();

    public MongoDBStorageEngineTest() {
        super("engine_tests");
    }

    @Override
    protected boolean valuesEqual(byte[] t1, byte[] t2) {
        if(t1.length != t2.length)
            return false;

        for(int i = 0; i < t1.length; i++) {
            if(t1[i] != t2[i])
                return false;
        }

        return true;
    }

    @Override
    public List<byte[]> getValues(int numValues) {
        List<byte[]> list = new ArrayList<byte[]>();
        for(int i = 0; i < numValues; i++) {
            list.add(mds.toBytes(new Doc("x", i)));
        }
        return list;

    }

    @Override
    public List<ByteArray> getKeys(int numKeys) {
        List<ByteArray> list = new ArrayList<ByteArray>();
        StringBuffer sb = new StringBuffer("key_");
        for(int i = 0; i < numKeys; i++) {
            sb.append(i);
            list.add(new ByteArray(sb.toString().getBytes()));
        }
        return list;
    }

    @Override
    public StorageEngine<ByteArray, byte[]> createStorageEngine(String name) {
        try {
            MongoDBStorageEngine e = new MongoDBStorageEngine(name);
            e.clearStore();
            return e;
        } catch(MongoDBException ee) {
            throw new VoldemortException(ee);
        }
    }
}
