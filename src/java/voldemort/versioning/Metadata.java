/*
 * Copyright © 2010 Nokia Corporation. All rights reserved.
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

package voldemort.versioning;

import java.util.Set;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.utils.Props;

/**
 * The metadata class provides a set of attributes associated with a Versioned
 * object. These attributes are stored as name-value pairs. These values are
 * kept separate from the value and can be used to store data associated with
 * the values. For example, metadata can be used to store information about the
 * value (such as its MIME type). Metadata can also be used by the system to
 * store information about how the value is represented (such as if it is
 * compressed or not and how).
 * 
 * @author mark
 * 
 */
@NotThreadsafe
public class Metadata {

    private volatile Props properties;

    public Metadata() {
        this.properties = new Props();
    }

    public Metadata(Metadata that) {
        this.properties = new Props();
        for(String prop: that.listProperties()) {
            setProperty(prop, that.getProperty(prop));
        }
    }

    @Override
    public Metadata clone() {
        return new Metadata(this);
    }

    public String getProperty(String name) {
        return properties.getString(name, null);
    }

    public void setProperty(String name, String value) {
        properties.put(name, value);
    }

    public void removeProperty(String name) {
        this.properties.remove(name);
    }

    public Set<String> listProperties() {
        return properties.keySet();
    }

    public int sizeInBytes() {
        return VectorClockProtoSerializer.sizeInBytes(this);
    }

    public byte[] toBytes() {
        return VectorClockProtoSerializer.toBytes(this);
    }

    @Override
    public boolean equals(Object object) {
        if(this == object)
            return true;
        if(object == null)
            return false;
        if(!object.getClass().equals(Metadata.class))
            return false;
        Metadata metadata = (Metadata) object;
        Set<String> props = listProperties();
        if(!props.equals(metadata.listProperties())) {
            return false;
        } else {
            for(String prop: props) {
                if(!this.getProperty(prop).equals(metadata.getProperty(prop))) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }
}
