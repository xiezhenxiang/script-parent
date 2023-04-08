package ai.plantdata.script.util.other;

import java.util.Set;

/**
 * @author Bovin
 * @description
 * @since 2020-07-23 15:06
 **/
public class ByteUtils {

    public static byte[] toBytes(Object value) {
        return KryoSerializer.serialize(value);
    }

    public static byte[] toBytes(Long id) {
        return KryoSerializer.serialize(id);
    }

    public static String toString(byte[] bytes) {
        return KryoSerializer.deserialize(bytes);
    }

    public static Long toLong(byte[] bytes) {
        return KryoSerializer.deserialize(bytes);
    }

    public static Byte toByte(byte[] bytes) {
        return KryoSerializer.deserialize(bytes);
    }

    public static void main(String[] args) {
        System.out.println(toLong(null));
    }

    public static byte[] toBytes(Set<Long> sets) {
        return KryoSerializer.serialize(sets);
    }

    public static Set<Long> toSet(byte[] bytes) {
        if(bytes == null){
            return null;
        }
        return KryoSerializer.deserialize(bytes);
    }
}
