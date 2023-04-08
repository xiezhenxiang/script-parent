package ai.plantdata.script.common.config;


import org.apache.commons.lang3.StringUtils;

import static ai.plantdata.script.common.config.NacosConfig.*;

public class EnvConstants {

    public final static String ENTITY_ANNOTATION = "entity_annotation";
    public final static String ENTITY_MERGE = "_entity_wait_merge";
    public final static String ATTRIBUTE_DEFINITION = "attribute_definition";
    public final static String BASIC_INFO = "basic_info";
    public final static String SYNONYMS = "synonyms";
    public final static String ATTRIBUTE_OBJECT = "attribute_object";
    public final static String ATTRIBUTE_OBJECT_EXT = "attribute_object_ext";
    public final static String ATTRIBUTE_PRIVATE_OBJECT = "attribute_private_object";
    public final static String ATTRIBUTE_FLOAT = "attribute_float";
    public final static String ATTRIBUTE_INTEGER = "attribute_integer";
    public final static String ATTRIBUTE_PRIVATE_DATA = "attribute_private_data";
    public final static String ATTRIBUTE_STRING = "attribute_string";
    public final static String ATTRIBUTE_SUMMARY = "attribute_summary";
    public final static String ATTRIBUTE_TEXT = "attribute_text";
    public final static String ATTRIBUTE_URL = "attribute_url";
    public final static String ATTRIBUTE_DATE_TIME = "attribute_date_time";
    public final static String SEMANTIC_DISTANCE = "semantic_distance";
    public final static String CHARSET = "utf-8";

    /** properties */
    private final static boolean windows = System.getProperty("os.name").toLowerCase().startsWith("windows");
    public static final String MONGO_DUMP_BIN_PATH = scriptV("mongo-dump-bin-path");
    public static final String SCRIPT_TMP_DIR = windows ? "D:/work/script/tmp/" :scriptV("script.tmp.dir").endsWith("/") ? scriptV("script.tmp.dir") : scriptV("script.tmp.dir") + "/";
    public static final String ETL_PLUGIN_DIR = scriptV("plugins-path").endsWith("/")
            ? scriptV("plugins-path") : scriptV("plugins-path") + "/";
    public static final String SHELL_DIR = windows ? "D:/work/script/azkaban_shell/" : scriptV("shell.directory");

    private static final String JDBC_PLATFORM = mysqlV("spring.sql.init.platform");
    private static final String MYSQL_IP = StringUtils.isBlank(mysqlV("jdbc.host")) ? mysqlV("mysql_ip") : mysqlV("jdbc.host");
    private static final String MYSQL_PORT = StringUtils.isBlank(mysqlV("jdbc.port")) ? mysqlV("mysql_port") : mysqlV("jdbc.port");
    public static final String KGMS_MYSQL_URL = getV("kgms.properties", "spring.datasource.url").replace("${spring.sql.init.platform}", JDBC_PLATFORM).replace("${mysql_ip}", MYSQL_IP).replace("${mysql_port}", MYSQL_PORT);
    public static final String XXL_JOB_MYSQL_URL = KGMS_MYSQL_URL.replace("kg_cloud_kgms", "xxl_job");
    public static final String DW_MYSQL_URL = KGMS_MYSQL_URL.replace("kg_cloud_kgms", "kg_cloud_kgdw");
    public static final String BOT_MYSQL_URL = KGMS_MYSQL_URL.replace("kg_cloud_kgms", "kg_cloud_kgbot");
    public static final String SEARCH_MYSQL_URL = KGMS_MYSQL_URL.replace("kg_cloud_kgms", "kg_cloud_kgsearch");
    public static final String SAS_MYSQL_URL = KGMS_MYSQL_URL.replace("kg_cloud_kgms", "kg_cloud_kgsas");
    public static final String DATA_LAKE_MYSQL_URL = KGMS_MYSQL_URL.replace("kg_cloud_kgms", "kg_cloud_datalake");
    public static final String MYSQL_USERNAME = mysqlV("spring.datasource.username");
    public static final String MYSQL_PASSWORD = mysqlV("spring.datasource.password");
    public static final String MONGODB_HOSTS = mongoV("mongo.addrs");
    public static final String MONGODB_USERNAME = mongoV("mongo.username");
    public static final String MONGODB_PASSWORD = mongoV("mongo.password");
    public static final String ES_HOSTS = esV("es.addrs");
    public static final String ES_USERNAME = esV("es.username");
    public static final String ES_PASSWORD = esV("es.password");
    public static final String REDIS_ADDRS = redisV("redis.addrs");
    public static final String REDIS_PASSWORD = redisV("redis.password");
    public static final Boolean OPEN_KG_LOG = "true".equalsIgnoreCase(kafkaV("kg.log.enable"));
    public static final String KAFKA_HOSTS = kafkaV("kafka.servers");
    public static final String KG_LOG_TOPIC = kafkaV("topic.kg.log");
    public static final String KG_SERVICE_LOG_TOPIC = kafkaV("topic.kg.service.log");
    public static final String FDFS_TRACKER_LIST = fastDfsV("fdfs.tracker-list");
}

