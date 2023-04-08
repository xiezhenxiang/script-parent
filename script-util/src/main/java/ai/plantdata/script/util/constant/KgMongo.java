package ai.plantdata.script.util.constant;

import ai.plantdata.script.common.config.EnvConstants;
import ai.plantdata.script.util.database.MongoUtil;

/**
 * @author xiezhenxiang 2020/7/30
 */
public class KgMongo {

    public static MongoUtil KG_MONGO = MongoUtil.getInstance(EnvConstants.MONGODB_HOSTS, EnvConstants.MONGODB_USERNAME, EnvConstants.MONGODB_PASSWORD);
}
