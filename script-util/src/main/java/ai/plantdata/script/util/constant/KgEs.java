package ai.plantdata.script.util.constant;

import ai.plantdata.script.common.config.EnvConstants;
import ai.plantdata.script.util.database.EsRestUtil;

/**
 * @author xiezhenxiang 2020/7/30
 */
public class KgEs {

    public static EsRestUtil KG_ES = EsRestUtil.getInstance(EnvConstants.ES_HOSTS, EnvConstants.ES_USERNAME, EnvConstants.ES_PASSWORD);
}
