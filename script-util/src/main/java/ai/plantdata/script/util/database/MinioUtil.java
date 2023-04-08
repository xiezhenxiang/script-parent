package ai.plantdata.script.util.database;

import ai.plantdata.script.util.other.HttpUtil;
import ai.plantdata.script.util.other.JacksonUtil;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import io.minio.messages.Part;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import static io.minio.ObjectWriteArgs.MIN_MULTIPART_SIZE;

/**
 * @author xiezhenxiang 2022/1/6
 */
public class MinioUtil {

    private static final ConcurrentHashMap<String, MinioUtil> MINIO_POOL = new ConcurrentHashMap<>(3);
    /**
     * 默认文件上传分片大小
     */
    private static final long DEFAULT_MULTIPART_SIZE = MIN_MULTIPART_SIZE * 2;
    /**
     * 最大分片数量
     */
    private static final int MAX_MULTIPART_COUNT = 10000;
    /**
     * 默认申请预签名直传url过期时间，单位秒
     */
    private static final int DEFAULT_PRE_SIGNED_URL_EXPIRE = 60 * 60 * 24;
    private final CustomMinioClient minioClient;

    private MinioUtil(CustomMinioClient minioClient) {
        this.minioClient = minioClient;
    }

    protected static class CustomMinioClient extends MinioClient {

        protected CustomMinioClient(MinioClient client) {
            super(client);
        }

        protected String getMultipartUploadId(String bucketName, String objectName) throws Exception {
            return createMultipartUpload(bucketName, null, objectName, null, null)
                    .result()
                    .uploadId();
        }

        protected List<Part> listParts(String bucketName, String objectName, String uploadId) throws Exception {
            return listParts(bucketName, null, objectName, MAX_MULTIPART_COUNT, 0, uploadId, null, null)
                    .result()
                    .partList();
        }

        protected void mergeMultiPart(String bucketName, String objectName, String uploadId, Part[] parts) throws Exception {
            completeMultipartUpload(bucketName, null, objectName,uploadId, parts, null, null);
        }
    }

    public static MinioUtil getInstance(String url, String userName, String password) {
        String key = url + userName + password;
        if (MINIO_POOL.containsKey(key)) {
            return MINIO_POOL.get(key);
        }
        MinioClient minioClient = CustomMinioClient.builder()
                .endpoint(url)
                .credentials(userName, password)
                .build();
        CustomMinioClient customMinioClient = new CustomMinioClient(minioClient);
        try {
            customMinioClient.bucketExists(BucketExistsArgs.builder().bucket("test").build());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("minio init fail");
        }
        MinioUtil minioUtil = new MinioUtil(customMinioClient);
        MINIO_POOL.put(key, minioUtil);
        return minioUtil;
    }

    /**
     * 创建桶
     */
    public void makeBucket(String name) {
        try {
            boolean b = minioClient.bucketExists(BucketExistsArgs.builder().bucket(name).build());
            if (!b) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(name).build());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 桶列表
     */
    public List<String> listBuckets() {
        try {
            return minioClient.listBuckets().stream().map(Bucket::name).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 删除桶
     */
    public void removeBucket(String name) {
        try {
            minioClient.removeBucket(RemoveBucketArgs.builder().bucket(name).build());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 桶文件列表
     * @param bucketName 桶名称
     * @param prefix 对象名称前缀
     * @param recursive 是否递归查找，查找子文件夹下的文件
     */
    public List<String> listObject(String bucketName, String prefix, boolean recursive) {
        try {
            List<String> ls = new ArrayList<>();
            ListObjectsArgs args = ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .recursive(recursive)
                    .build();
            for (Result<Item> itemResult : minioClient.listObjects(args)) {
                Item item = itemResult.get();
                if (!item.isDir()) {
                    ls.add(item.objectName());
                }
            }
            return ls;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 以流的形式下载一个对象
     * @param bucketName 桶名称
     * @param objectName 文件名称
     */
    public InputStream getObject(String bucketName, String objectName) {
        GetObjectArgs args = GetObjectArgs.builder().bucket(bucketName).object(objectName).build();
        try {
            return minioClient.getObject(args);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 下载一个对象并写入OutputStream
     * @param bucketName 桶名称
     * @param objectName 文件名称
     */
    public void getObject(String bucketName, String objectName, OutputStream out) {
        InputStream input = getObject(bucketName, objectName);
        byte[] bts = new byte[4096];
        int len;
        try {
            while ((len = input.read(bts)) > 0) {
                out.write(bts, 0, len);
            }
            out.flush();
            input.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件
     */
    public void removeObject(String bucketName, String... objectNames) {
        try {
            for (String objectName : objectNames) {
                minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 通过流上传文件
     */
    public void putObject(String bucketName, String objectName, InputStream inputStream) {
        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .stream(inputStream, -1, DEFAULT_MULTIPART_SIZE)
                .build();
        try {
            minioClient.putObject(putObjectArgs);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 上传本地文件
     */
    public void putObject(String bucketName, String objectName, File file) {
        try {
            FileInputStream inputStream = new FileInputStream(file);
            putObject(bucketName, objectName, inputStream);
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 生成一个给HTTP请求用的预签名URL
     * 大文件直传
     * @param method get请求下载，put请求上传
     */
    private String getPreSignedUrl(String bucket, String objectName, Method method, Map<String, String> queryParams) {
        GetPresignedObjectUrlArgs args = GetPresignedObjectUrlArgs.builder()
                .bucket(bucket)
                .method(method)
                .object(objectName)
                .extraQueryParams(queryParams)
                .expiry(DEFAULT_PRE_SIGNED_URL_EXPIRE)
                .build();
        try {
            return minioClient.getPresignedObjectUrl(args);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 获取文件下载直传Url
     */
    public String preSignedDownloadUrl(String bucket, String objectName) {
        return getPreSignedUrl(bucket, objectName, Method.GET, null);
    }

    /**
     * 获取文件上传直传Url
     */
    public String preSignedUploadUrl(String bucket, String objectName) {
        return getPreSignedUrl(bucket, objectName, Method.PUT, null);
    }

    /**
     * 获取文件分片上传ID
     */
    public String getMultipartUploadId(String bucketName, String objectName) {
        try {
            return minioClient.getMultipartUploadId(bucketName, objectName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 获取分片上传Url
     * @param uploadId 分片ID
     * @param partNum 分片数量
     */
    public List<String> preSignedMultiPartUploadUrl(String bucket, String objectName, String uploadId, int partNum) {
        List<String> ls = new ArrayList<>();
        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("uploadId", uploadId);
        for (int i = 1; i <= partNum; i++) {
            queryMap.put("partNumber", String.valueOf(i));
            ls.add(getPreSignedUrl(bucket, objectName, Method.PUT, queryMap));
        }
        return ls;
    }

    /**
     * 分片合并
     **/
    public void mergeMultipartUpload(String bucketName, String objectName, String uploadId) {
        Part[] parts = new Part[MAX_MULTIPART_COUNT];
        try {
            List<Part> ls = minioClient.listParts(bucketName, objectName, uploadId);
            int partNumber = 1;
            for (Part part : ls) {
                parts[partNumber - 1] = new Part(partNumber, part.etag());
                partNumber++;
            }
            if (!ls.isEmpty()) {
                minioClient.mergeMultiPart(bucketName, objectName, uploadId, parts);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public CustomMinioClient getMinioClient() {
        return minioClient;
    }

    public static void main(String[] args) throws Exception {
        MinioUtil minioClient = MinioUtil.getInstance("http://192.168.4.119:9001", "root", "root@hiekn");
        File file = new File("C:\\Users\\14307\\Desktop\\gg1.jpg");
        List<String> parts = new ArrayList<>();
        long byteSize = 1024 * 1024 * 10;
        int count = (int) Math.ceil(file.length() / (double) byteSize);
        RandomAccessFile rf = new RandomAccessFile(file, "r");

        for (int i = 0; i < count; i++) {
            String partFileName = file.getName() + ".part" + i;
            System.out.println("分片名称：" + partFileName);
            byte[] b = new byte[(int)byteSize];
            rf.seek( i * byteSize);
            int s = rf.read(b);
            FileOutputStream out = new FileOutputStream(partFileName);
            out.write(b, 0, s);
            out.flush();
            out.close();
            parts.add(partFileName);
        }

        String bucket = "123456", objectName = "gg.jpg";
        String uploadId = minioClient.getMultipartUploadId(bucket, objectName);
        List<String> urls = minioClient.preSignedMultiPartUploadUrl(bucket, objectName, uploadId, count);
        System.out.println(JacksonUtil.writeValueAsString(urls));

        for (int i = 0; i < parts.size(); i++) {
            HttpUtil.sendBinary(urls.get(i), "PUT",null, new File(parts.get(i)));
        }
        minioClient.mergeMultipartUpload(bucket, objectName, uploadId);
    }
}
