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

package voldemort.client;

import voldemort.store.invalidmetadata.MetadataCheckingStore;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

public class MetadataRefreshingStore<K, V, T> extends MetadataCheckingStore<K, V, T> {

    private final StoreClientFactory storeFactory;
    private final InconsistencyResolver<Versioned<V>> resolver;

    public MetadataRefreshingStore(String name,
                                   InconsistencyResolver<Versioned<V>> resolver,
                                   StoreClientFactory factory,
                                   int retries) {
        super(name, retries);
        this.storeFactory = factory;
        this.resolver = resolver;
        reinit();
    }

    @Override
    protected void reinit() {
        innerStore = storeFactory.getRawStore(getName(), resolver);

    }
}
