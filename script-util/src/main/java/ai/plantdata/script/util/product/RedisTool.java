package ai.plantdata.script.util.product;

import ai.plantdata.script.common.config.EnvConstants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;

public class RedisTool {

    public static RedisTemplate<String, String> REDIS_TEMPLATE = new RedisTemplate<>();
    public static StringRedisTemplate STRING_REDIS_TEMPLATE = new StringRedisTemplate();

    static  {

        JedisConnectionFactory jedisConnectionFactory;
        String[] addressArr = EnvConstants.REDIS_ADDRS.split(",");
        if (addressArr.length > 1) {
            RedisClusterConfiguration configuration = new RedisClusterConfiguration();
            List<RedisNode> ls = new ArrayList<>();
            for (String ipPortStr : addressArr) {
                String[] ipPortArr = ipPortStr.split(":");
                ls.add(new RedisNode(ipPortArr[0], Integer.parseInt(ipPortArr[1])));
            }
            configuration.setClusterNodes(ls);
            if (StringUtils.isNoneBlank(EnvConstants.REDIS_PASSWORD)) {
                configuration.setPassword(EnvConstants.REDIS_PASSWORD);
            }
            jedisConnectionFactory = new JedisConnectionFactory(configuration);
        } else {
            String[] ipPortArr = addressArr[0].split(":");
            RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(ipPortArr[0], Integer.parseInt(ipPortArr[1]));
            if (StringUtils.isNoneBlank(EnvConstants.REDIS_PASSWORD)) {
                configuration.setPassword(EnvConstants.REDIS_PASSWORD);
            }
            jedisConnectionFactory = new JedisConnectionFactory(configuration);
        }
        jedisConnectionFactory.afterPropertiesSet();
        REDIS_TEMPLATE.setConnectionFactory(jedisConnectionFactory);
        REDIS_TEMPLATE.afterPropertiesSet();

        STRING_REDIS_TEMPLATE.setConnectionFactory(jedisConnectionFactory);
        STRING_REDIS_TEMPLATE.afterPropertiesSet();

    }

    public static void main(String[] args) {

        String key = "channel_resourceDelete";
        Object obj = STRING_REDIS_TEMPLATE.hasKey(key);
        System.out.println(obj);
        System.out.println(STRING_REDIS_TEMPLATE.opsForHash().entries(key));
        STRING_REDIS_TEMPLATE.delete(key);
    }
}
