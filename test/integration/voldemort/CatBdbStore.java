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

package voldemort;

import java.io.File;
import java.util.Iterator;

import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.serialization.StringSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageEngine;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.Props;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;

public class CatBdbStore {

    public static void main(String[] args) throws Exception {
        if(args.length != 2)
            Utils.croak("USAGE: java " + CatBdbStore.class.getName() + " bdb_dir" + " storeName"
                        + " server.properties.path");

        String bdbDir = args[0];
        String storeName = args[1];
        String serverProperties = args[2];

        VoldemortConfig config = new VoldemortConfig(new Props(new File(serverProperties)));
        config.setBdbCursorPreload(false);

        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setTxnNoSync(true);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(config.isBdbWriteTransactionsEnabled());
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(config.isBdbWriteTransactionsEnabled());
        databaseConfig.setSortedDuplicates(config.isBdbSortedDuplicatesEnabled());

        BdbStorageConfiguration storeConfiguration = new BdbStorageConfiguration(config,
                                                                                 environmentConfig,
                                                                                 databaseConfig);
        StorageEngine<ByteArray, byte[], byte[]> store = storeConfiguration.getStore(TestUtils.getStoreDef(storeName,
                                                                                                           BdbStorageConfiguration.TYPE_NAME));
        StorageEngine<String, String, String> stringStore = SerializingStorageEngine.wrap(store,
                                                                                          new StringSerializer(),
                                                                                          new StringSerializer(),
                                                                                          new StringSerializer());
        Iterator<Pair<String, Versioned<String>>> iter = stringStore.entries(null,
                                                                             new DefaultVoldemortFilter<String, String>(),
                                                                             null);
        while(iter.hasNext()) {
            Pair<String, Versioned<String>> entry = iter.next();
            System.out.println(entry.getFirst() + " => " + entry.getSecond().getValue());
        }
    }
}
