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

package voldemort.client.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.pb.VProto.DeleteResponse;
import voldemort.client.protocol.pb.VProto.GetAllResponse;
import voldemort.client.protocol.pb.VProto.GetResponse;
import voldemort.client.protocol.pb.VProto.GetVersionResponse;
import voldemort.client.protocol.pb.VProto.PutResponse;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.server.RequestRoutingType;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * The client side of the protocol buffers request format
 * 
 * 
 */
public class ProtoBuffClientRequestFormat implements RequestFormat {

    protected final ErrorCodeMapper mapper;

    // **TODO: The code for these read/write/complete methods have lots of
    // commonality that
    // could be factored into a generic base class for all PB-encoded requests

    public ProtoBuffClientRequestFormat() {
        this.mapper = new ErrorCodeMapper();
    }

    public void writeDeleteRequest(DataOutputStream output,
                                   String storeName,
                                   ByteArray key,
                                   Version version,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.DELETE)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setDelete(VProto.DeleteRequest.newBuilder()
                                                                                      .setKey(ByteString.copyFrom(key.get()))
                                                                                      .setVersion(ProtoUtils.encodeClock(version)))
                                                       .build());
    }

    public boolean isCompleteDeleteResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public boolean readDeleteResponse(DataInputStream input) throws IOException {
        DeleteResponse.Builder response = ProtoUtils.readToBuilder(input,
                                                                   DeleteResponse.newBuilder());
        if(response.hasError())
            ProtoUtils.throwException(mapper, response.getError());
        return response.getSuccess();
    }

    public ClientRequestFormat<List<Versioned<byte[]>>> createGetRequest(final String storeName,
                                                                         final ByteArray key,
                                                                         final byte[] transforms,
                                                                         final RequestRoutingType routingType) {
        return new ClientRequestFormat<List<Versioned<byte[]>>>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeGetRequest(outputStream, storeName, key, transforms, routingType);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteGetResponse(buffer);
            }

            public List<Versioned<byte[]>> readResponse(DataInputStream inputStream)
                    throws IOException {
                return readGetResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<Map<ByteArray, List<Versioned<byte[]>>>> createGetAllRequest(final String storeName,
                                                                                            final Iterable<ByteArray> key,
                                                                                            final Map<ByteArray, byte[]> transforms,
                                                                                            final RequestRoutingType routingType) {
        return new ClientRequestFormat<Map<ByteArray, List<Versioned<byte[]>>>>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeGetAllRequest(outputStream, storeName, key, transforms, routingType);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteGetAllResponse(buffer);
            }

            public Map<ByteArray, List<Versioned<byte[]>>> readResponse(DataInputStream inputStream)
                    throws IOException {
                return readGetAllResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<List<Version>> createGetVersionsRequest(final String storeName,
                                                                       final ByteArray key,
                                                                       final RequestRoutingType routingType) {
        return new ClientRequestFormat<List<Version>>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeGetVersionRequest(outputStream, storeName, key, routingType);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteGetVersionResponse(buffer);
            }

            public List<Version> readResponse(DataInputStream inputStream) throws IOException {
                return readGetVersionResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<Boolean> createDeleteRequest(final String storeName,
                                                            final ByteArray key,
                                                            final Version version,
                                                            final RequestRoutingType routingType) {
        return new ClientRequestFormat<Boolean>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeDeleteRequest(outputStream, storeName, key, version, routingType);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteDeleteResponse(buffer);
            }

            public Boolean readResponse(DataInputStream inputStream) throws IOException {
                return readDeleteResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<Version> createPutRequest(final String storeName,
                                                         final ByteArray key,
                                                         final Versioned<byte[]> value,
                                                         final byte[] transforms,
                                                         final RequestRoutingType routingType) {
        return new ClientRequestFormat<Version>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writePutRequest(outputStream, storeName, key, value, transforms, routingType);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompletePutResponse(buffer);
            }

            public Version readResponse(DataInputStream inputStream) throws IOException {
                return readPutResponse(inputStream);
            }
        };
    }

    public void writeGetRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        VProto.GetRequest.Builder get = VProto.GetRequest.newBuilder();
        get.setKey(ByteString.copyFrom(key.get()));

        if(transforms != null) {
            get.setTransforms(ByteString.copyFrom(transforms));
        }

        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.GET)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setGet(get)
                                                       .build());
    }

    public boolean isCompleteGetResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream input) throws IOException {
        GetResponse.Builder response = ProtoUtils.readToBuilder(input, GetResponse.newBuilder());
        if(response.hasError())
            ProtoUtils.throwException(mapper, response.getError());
        return ProtoUtils.decodeVersions(response.getVersionedList());
    }

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> keys,
                                   Map<ByteArray, byte[]> transforms,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKeys(keys);
        VProto.GetAllRequest.Builder req = VProto.GetAllRequest.newBuilder();
        for(ByteArray key: keys)
            req.addKeys(ByteString.copyFrom(key.get()));

        if(transforms != null) {
            for(Map.Entry<ByteArray, byte[]> transform: transforms.entrySet()) {
                req.addTransforms(VProto.GetAllRequest.GetAllTransform.newBuilder()
                                                                      .setKey(ByteString.copyFrom(transform.getKey()
                                                                                                           .get()))
                                                                      .setTransform(ByteString.copyFrom(transform.getValue())));
            }
        }
        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.GET_ALL)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setGetAll(req)
                                                       .build());
    }

    public boolean isCompleteGetAllResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream input)
            throws IOException {
        GetAllResponse.Builder response = ProtoUtils.readToBuilder(input,
                                                                   GetAllResponse.newBuilder());
        if(response.hasError())
            ProtoUtils.throwException(mapper, response.getError());
        Map<ByteArray, List<Versioned<byte[]>>> vals = new HashMap<ByteArray, List<Versioned<byte[]>>>(response.getValuesCount());
        for(VProto.KeyedVersions versions: response.getValuesList())
            vals.put(ProtoUtils.decodeBytes(versions.getKey()),
                     ProtoUtils.decodeVersions(versions.getVersionsList()));
        return vals;
    }

    public void writePutRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                Versioned<byte[]> versioned,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        VProto.PutRequest.Builder req = VProto.PutRequest.newBuilder()
                                                         .setKey(ByteString.copyFrom(key.get()))
                                                         .setVersioned(ProtoUtils.encodeVersioned(versioned));
        if(transforms != null)
            req = req.setTransforms(ByteString.copyFrom(transforms));
        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.PUT)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setPut(req)
                                                       .build());
    }

    public boolean isCompletePutResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public Version readPutResponse(DataInputStream input) throws IOException {
        PutResponse.Builder response = ProtoUtils.readToBuilder(input, PutResponse.newBuilder());
        if(response.hasError()) {
            ProtoUtils.throwException(mapper, response.getError());
        }
        if(response.hasVersion()) {
            return ProtoUtils.decodeClock(response.getVersion());
        } else {
            return null;
        }
    }

    public boolean isCompleteGetVersionResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer);
    }

    public List<Version> readGetVersionResponse(DataInputStream stream) throws IOException {
        GetVersionResponse.Builder response = ProtoUtils.readToBuilder(stream,
                                                                       GetVersionResponse.newBuilder());
        if(response.hasError())
            ProtoUtils.throwException(mapper, response.getError());
        List<Version> versions = Lists.newArrayListWithCapacity(response.getVersionsCount());
        for(VProto.VectorClock version: response.getVersionsList())
            versions.add(ProtoUtils.decodeClock(version));
        return versions;
    }

    public void writeGetVersionRequest(DataOutputStream output,
                                       String storeName,
                                       ByteArray key,
                                       RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        ProtoUtils.writeMessage(output,
                                VProto.VoldemortRequest.newBuilder()
                                                       .setType(RequestType.GET_VERSION)
                                                       .setStore(storeName)
                                                       .setShouldRoute(routingType.equals(RequestRoutingType.ROUTED))
                                                       .setRequestRouteType(routingType.getRoutingTypeCode())
                                                       .setGet(VProto.GetRequest.newBuilder()
                                                                                .setKey(ByteString.copyFrom(key.get())))
                                                       .build());
    }

    private boolean isCompleteResponse(ByteBuffer buffer) {
        int size = buffer.getInt();
        return buffer.remaining() == size;
    }
}
