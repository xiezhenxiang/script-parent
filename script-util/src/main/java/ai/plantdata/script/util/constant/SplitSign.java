package ai.plantdata.script.util.constant;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 不易重复的分隔符
 * @author xiezhenxiang 2021/11/1
 */
public class SplitSign {

    public static final String splitChar = "╎";

    public static void main(String[] args) {
        String str = "张珊" + splitChar + "ssssf是是1算法是";
        System.out.println(DigestUtils.md5Hex(str));
        System.out.println(getMD5Str(str));
    }

    public static String getMD5Str(String str) {
        byte[] digest = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("md5");
            digest  = md5.digest(str.getBytes("utf-8"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        //16是表示转换为16进制数
        String md5Str = new BigInteger(1, digest).toString(16);
        return md5Str;
    }
}
