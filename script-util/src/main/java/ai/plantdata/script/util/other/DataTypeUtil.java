package ai.plantdata.script.util.other;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Date;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * @author xiezhenxiang 2020/7/30
 */
public class DataTypeUtil {

    public static boolean intCheck(String str) {
        return str != null && Pattern.matches("[\\d]*$", str);
    }

    public static boolean doubleCheck(String str) {
        return str != null && Pattern.matches("(\\d+)(\\.\\d+)?$", str);
    }

    public static boolean dateCheck(String str) {
        return str != null && Pattern.matches("^\\d{4}[-]\\d{1,2}[-]\\d{1,2}[ ]{0,1}\\d{0,2}[:]{0,1}\\d{0,2}[:]{0,1}\\d{0,2}[:]{0,1}\\d{0,3}$", str);
    }
}
