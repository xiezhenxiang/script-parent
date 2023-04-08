package ai.plantdata.script.util.constant;

import ai.plantdata.script.common.config.EnvConstants;
import ai.plantdata.script.util.database.DriverUtil;

/**
 * @author xiezhenxiang 2020/7/30
 */
public class KgMySql {

    public static DriverUtil KG_MYSQL = DriverUtil.getInstance(EnvConstants.KGMS_MYSQL_URL, EnvConstants.MYSQL_USERNAME, EnvConstants.MYSQL_PASSWORD);
}
