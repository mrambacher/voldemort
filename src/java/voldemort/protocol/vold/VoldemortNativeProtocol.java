package voldemort.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;

import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

public class VoldemortNativeProtocol {

    public static int getKeyRequestSize(ByteArray key) {
        if(key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        }
        // 4 bytes for the size preamble, N bytes for the actual key data...
        return 4 + key.length();
    }

    public static void writeKey(DataOutputStream outputStream, ByteArray key) throws IOException {
        outputStream.writeInt(key.length());
        outputStream.write(key.get());
    }

    public static ByteArray readKey(DataInputStream stream) throws IOException {
        int keySize = stream.readInt();
        byte[] key = new byte[keySize];
        stream.readFully(key);
        return new ByteArray(key);
    }

    public static int getVersionRequestSize(Version version) {
        // 4 bytes for the size preamble, 0 or N bytes for the actual version
        // data...
        int size = 4;
        if(version != null) {
            size += version.sizeInBytes();
        }
        return size;
    }

    public static Version readVersion(DataInputStream inputStream) throws IOException {
        int versionSize = inputStream.readInt();
        if(versionSize > 0) {
            byte[] bytes = new byte[versionSize];
            ByteUtils.read(inputStream, bytes);
            return VersionFactory.toVersion(bytes);
        } else {
            return null;
        }
    }

    public static Versioned<byte[]> readVersioned(DataInputStream inputStream) throws IOException {
        int valueSize = inputStream.readInt();
        byte[] bytes = new byte[valueSize];
        ByteUtils.read(inputStream, bytes);

        return VersionFactory.toVersioned(bytes);
    }

    public static List<Versioned<byte[]>> readVersioneds(DataInputStream inputStream)
            throws IOException {
        int resultSize = inputStream.readInt();
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(resultSize);
        for(int i = 0; i < resultSize; i++) {
            results.add(readVersioned(inputStream));
        }
        return results;
    }

    public static int getStringRequestSize(String s) throws IOException {
        if(s == null) {
            throw new IllegalArgumentException("String cannot be null.");
        }
        ByteArrayOutputStream baos = null;
        DataOutputStream dos = null;
        try {
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
            dos.writeUTF(s);
            return baos.toByteArray().length;
        } finally {
            if(dos != null) {
                dos.close();
            }
            if(baos != null) {
                baos.close();
            }
        }
    }

}
