package ai.plantdata.script.util.constant;

import ai.plantdata.script.common.config.NacosConfig;
import ai.plantdata.script.util.database.MinioUtil;

import java.util.UUID;

/**
 * @author xiezhenxiang 2022/3/7
 */
public class KgMinio {

    public static final String DEFAULT_BUCKET = "file-system";

    public static final MinioUtil KG_MINIO = MinioUtil.getInstance(
            NacosConfig.minioV("minio.endpoint"),
            NacosConfig.minioV("minio.access-key"),
            NacosConfig.minioV("minio.secret-key"));

    public static String getUploadName(String fileName) {
        String uuIdStr = UUID.randomUUID().toString().replace("-", "");
        if (fileName.contains(".")) {
            String extName = fileName.substring(fileName.lastIndexOf(".") + 1);
            return uuIdStr.concat(".").concat(extName);
        }
        return uuIdStr;
    }
}
