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

package voldemort.versioning;

/**
 * An interface that allows us to determine if a given version happened before
 * or after another version.
 * 
 * This could have been done using the comparable interface but that is
 * confusing, because the numeric codes are easily confused, and because
 * concurrent versions are not necessarily "equal" in the normal sense.
 * 
 * 
 */
public interface Version {

    /**
     * Return whether or not the given version preceeded this one, succeeded it,
     * or is concurrant with it
     * 
     * @param v The other version
     */
    public Occured compare(Version v);

    /**
     * Return the serialized representation of the version,
     */
    public byte[] toBytes();

    /**
     * Return the size in bytes of the serialized representation of the version.
     */
    public int sizeInBytes();

    /**
     * Merges the current clock with the input clock and returns a new merged
     * clock
     * 
     * @param that The clock to be added to the existing clock.
     * @return The merged clock
     */
    public Version merge(Version that);

    /**
     * Increments the version for the input nodeId
     * 
     * @param nodeId The node being incremented
     * @oparam time The new timestamp for the version
     */
    public void incrementClock(int nodeId, long time);

    /**
     * Increments the version for the input nodeId
     * 
     * @param nodeId The node being incremented
     */
    public void incrementClock(int nodeId);

    /**
     * Returns the timestamp for the version
     * 
     * @return The timestamp of the version.
     */
    public long getTimestamp();

    /**
     * Returns a copy of the current object with the nodeId incremented
     * 
     * @param nodeId The node to increment the clock for
     * @param time The new timestamp for the clock
     * @return
     */
    public Version incremented(int nodeId, long time);
}
