package ai.plantdata.script.util.other;

/**
 * @author xiezhenxiang 2022/3/8
 */
public class SystemUtil {

    public static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }
}
