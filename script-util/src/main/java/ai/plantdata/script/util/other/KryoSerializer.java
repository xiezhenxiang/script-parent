package ai.plantdata.script.util.other;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * @author Bovin
 * @description
 * @since 2020-07-27 14:20
 **/
public class KryoSerializer {

    private static final ThreadLocal<Kryo> kryoLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setReferences(true);// 默认值为 true, 强调作用
        kryo.setRegistrationRequired(false);// 默认值为 false, 强调作用
        return kryo;
    });

    public static byte[] serialize(Object obj) {
        if (obj == null) {
            return null;
        }
        Kryo kryo = kryoLocal.get();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Output output = new Output(byteArrayOutputStream);
        kryo.writeClassAndObject(output, obj);
        output.close();
        return byteArrayOutputStream.toByteArray();
    }

    public static <T> T deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        Kryo kryo = kryoLocal.get();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        Input input = new Input(byteArrayInputStream);
        input.close();
        return (T) kryo.readClassAndObject(input);
    }
}
