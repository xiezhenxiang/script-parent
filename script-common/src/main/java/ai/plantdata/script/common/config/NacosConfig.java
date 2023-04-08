package ai.plantdata.script.common.config;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author xiezhenxiang 2021/1/23
 */
@Slf4j
public class NacosConfig {

    private static final String group = "DEFAULT_GROUP";
    private static String serverAddress;
    private static final HashMap<String, String> SCRIPT_CONFIG = new HashMap<>();
    private static final HashMap<String, String> MYSQL_CONFIG = new HashMap<>();
    private static final HashMap<String, String> MONGO_CONFIG = new HashMap<>();
    private static final HashMap<String, String> ES_CONFIG = new HashMap<>();
    private static final HashMap<String, String> REDIS_CONFIG = new HashMap<>();
    private static final HashMap<String, String> KAFKA_CONFIG = new HashMap<>();
    private static final HashMap<String, String> FASTDFS_CONFIG = new HashMap<>();
    private static final HashMap<String, String> MINIO_CONFIG = new HashMap<>();
    private static final CountDownLatch countDown = new CountDownLatch(8);


    static {
        serverAddress = System.getProperty("nacos.addr");
        if (StringUtils.isBlank(serverAddress)) {
            serverAddress = System.getProperty("spring.cloud.nacos.server-addr");
        }
        initConfig("script.properties", SCRIPT_CONFIG);
        initConfig("datasource.properties", MYSQL_CONFIG);
        initConfig("mongodb.properties", MONGO_CONFIG);
        initConfig("elasticsearch.properties", ES_CONFIG);
        initConfig("redis.properties", REDIS_CONFIG);
        initConfig("kafka.properties", KAFKA_CONFIG);
        initConfig("fastdfs.properties", FASTDFS_CONFIG);
        initConfig("minio.properties", MINIO_CONFIG);
    }

    public static String getV(String dataId, String key) {
        HashMap<String, String> m = new HashMap<>();
        getContent(dataId, m);
        return getV(m, key);
    }

    private static void initConfig(String dataId, HashMap<String, String> configMap) {
        Thread thread = new Thread(() -> {
            getContent(dataId, configMap);
            countDown.countDown();
        });
        thread.start();
    }

    private static void getContent(String dataId, HashMap<String, String> configMap) {
        try {
            Properties properties = new Properties();
            properties.put(PropertyKeyConst.SERVER_ADDR, serverAddress);
            ConfigService configService = NacosFactory.createConfigService(properties);
            String content = configService.getConfig(dataId, group, 100);
            if (content != null) {
                String[] lines = content.split("\n");
                for (String line : lines) {
                    line = line.trim();
                    int splitIndex = line.indexOf("=");
                    if (!line.startsWith("#") && splitIndex < line.length() - 1) {
                        String k = line.substring(0, splitIndex).trim();
                        String v = line.substring(splitIndex + 1).trim();
                        configMap.put(k, v);
                    }
                }
                configService.shutDown();
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("read nacos content fail，dataId:{}", dataId);
        }
    }

    public static String scriptV(String key) {
        return getV(SCRIPT_CONFIG, key);
    }

    public static String mysqlV(String key) {
        return getV(MYSQL_CONFIG, key);
    }

    public static String mongoV(String key) {
        return getV(MONGO_CONFIG, key);
    }

    public static String esV(String key) {
        return getV(ES_CONFIG, key);
    }

    public static String redisV(String key) {
        return getV(REDIS_CONFIG, key);
    }

    public static String kafkaV(String key) {
        return getV(KAFKA_CONFIG, key);
    }

    public static String fastDfsV(String key) {
        return getV(FASTDFS_CONFIG, key);
    }

    public static String minioV(String key) {
        return getV(MINIO_CONFIG, key);
    }

    private static String getV(HashMap<String, String> config, String key) {
        String v;
        if (System.getProperty(key) != null) {
            v= System.getProperty(key);
        } else {
            try {
                countDown.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            v = config.getOrDefault(key, "");
        }
        v = v.replace("{cipher}", "");
        return v;
    }

    private static NamingService namingService;
    public static String getInstance(String serviceName) {

        String address = null;
        try {
            if (namingService == null) {
                namingService = NamingFactory.createNamingService(serverAddress);
            }
            List<Instance> allInstances = namingService.getAllInstances(serviceName);
            for (Instance instance : allInstances) {
                if (instance.isHealthy()) {
                    address = instance.getIp() + ":" + instance.getPort();
                    break;
                }
            }
        } catch (NacosException e) {
            e.printStackTrace();
        }
        return address;
    }
}