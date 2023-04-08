package ai.plantdata.script.util.other;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Jackson util
 * 1、obj need private and set/get；
 * 2、do not support inner class；
 * @author xuxueli 2015-9-25 18:02:56
 */
public class JacksonUtil {
	private static Logger logger = LoggerFactory.getLogger(JacksonUtil.class);

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
		OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		// not serial null value
		OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
		OBJECT_MAPPER.setTimeZone(TimeZone.getTimeZone("GMT+8"));
	}

	public static ObjectMapper getInstance() {
        return OBJECT_MAPPER;
    }

    /**
     * bean、array、List、Map --> json
     * @param obj obj
     * @return json string
     */
    public static String writeValueAsString(Object obj) {
    	try {
			return getInstance().writeValueAsString(obj);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
        return null;
    }

    /**
     * string --> bean、Map、List(array)
     * @return obj
     */
    public static <T> T readValue(String jsonStr, Class<T> clazz) {
    	try {
			return getInstance().readValue(jsonStr, clazz);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
    	return null;
    }

	/**
	 * obj --> bean、Map、List(array)
	 * @return obj
	 */
	public static <T> T convertValue(Object obj, Class<T> clazz) {
		return readValue(writeValueAsString(obj), clazz);
	}

	/**
	 * string --> List<Bean>...
	 * @param jsonStr
	 * @param parametrized
	 * @param parameterClasses
	 * @param <T>
	 * @return
	 */
	public static <T> T readValue(String jsonStr, Class<?> parametrized, Class<?>... parameterClasses) {
		try {
			JavaType javaType = getInstance().getTypeFactory().constructParametricType(parametrized, parameterClasses);
			return getInstance().readValue(jsonStr, javaType);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

    public static <T> T readValueRefer(String jsonStr, TypeReference type) {
    	try {
			return getInstance().readValue(jsonStr, new TypeReference<T>() { });
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
    	return null;
    }

    public static void main(String[] args) {
		try {
			Map<String, Object> map = new HashMap<>();
			map.put("str", null);
			map.put("int", 1);
			map.put("float1", 1.0);
			map.put("float2", 1.1);
			String json = writeValueAsString(map);
			System.out.println(json);
			System.out.println(readValue(json, Map.class));
			json = writeValueAsString(Lists.newArrayList(map));
			List<Map<String, Object>> ls = readValue(json, List.class, Map.class);
			System.out.println(writeValueAsString(ls));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}