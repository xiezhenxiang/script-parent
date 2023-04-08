package ai.plantdata.script.util.database;

import ai.plantdata.script.util.other.JacksonUtil;
import ai.plantdata.script.util.other.Snowflake;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Low Level RestClient support es 5.x
 * @author xiezhenxiang 2019/9/7
 **/
@Slf4j
public class EsRestUtil {

    private RestClient client;
    private final HttpHost[] hosts;
    private final String userName;
    private final String password;
    private static final ConcurrentHashMap<String, RestClient> pool = new ConcurrentHashMap<>(10);
    private String scrollIntervalTime = "5m";

    private static final HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
            new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(1024 * 1024 * 1024);

    public static EsRestUtil getInstance(String hostsStr, String userName, String password) {
        String[] ipArr = hostsStr.split(",");
        HttpHost[] hosts = new HttpHost[ipArr.length];
        for (int i = 0; i < ipArr.length; i++) {
            String[] ipPort = ipArr[i].split(":");
            hosts[i] = new HttpHost(ipPort[0], Integer.parseInt(ipPort[1]));
        }
        return new EsRestUtil(userName, password, hosts);
    }

    public void setScrollIntervalTime(String intervalTime) {
        this.scrollIntervalTime = intervalTime;
    }

    public void createIndex(String index, String mapping) {

        initClient();
        String endpoint ="/" + index;
        NStringEntity entity = new NStringEntity(mapping, ContentType.APPLICATION_JSON);
        try {   
            client.performRequest("PUT", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic create index fail!");
        }
    }

    public void deleteIndex(String index) {

        initClient();
        String endpoint ="/" + index;
        try {
            client.performRequest("DELETE", endpoint);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic delete index fail!");
        }
    }

    /**
     * add a new type to an existing index
     * @author xiezhenxiang 2019/9/12
     **/
    public void createMappings(String index, String mappings) {

        initClient();
        JSONObject para = new JSONObject();
        para.put("mappings", JSONObject.parseObject(mappings));

        String endpoint ="/" + index;
        NStringEntity entity = new NStringEntity(para.toJSONString(), ContentType.APPLICATION_JSON);
        try {
            client.performRequest("PUT", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic create mappings fail!");
        }
    }

    /**
     * add new fields to an existing type
     * @author xiezhenxiang 2019/9/12
     **/
    public void putMapping(String index, String type, String properties) {

        initClient();

        String endpoint ="/" + index + "/_mapping/" + type;
        NStringEntity entity = new NStringEntity(properties, ContentType.APPLICATION_JSON);
        try {
            client.performRequest("PUT", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic put mapping fail!");
        }
    }

    public void reindex(String sourceIndex, String sourceType, String destIndex) {
        reindex( null, sourceIndex, sourceType, destIndex, null, 3000);
    }

    /**
     * move index's data to another
     * @author xiezhenxiang 2019/9/10
     **/
    public void reindex(String sourceHostUri, String sourceIndex, String sourceType, String destIndex, String query, Integer batchSize) {

        initClient();
        String endpoint ="/_reindex?slices=5";

        JSONObject para = new JSONObject();
        para.put("conflicts", "proceed");
        JSONObject source = new JSONObject();
        JSONObject dest = new JSONObject();

        if (StringUtils.isNotBlank(sourceHostUri)) {

            JSONObject remote = new JSONObject();
            remote.put("host", sourceHostUri);
            source.put("remote", remote);
        }

        source.put("index", sourceIndex);

        if (StringUtils.isNotBlank(sourceType)) {
            source.put("type", sourceType);
        }
        if (StringUtils.isNotBlank(query)) {
            source.put("query", JSONObject.parseObject(query));
        }
        if (batchSize != null) {
            source.put("size", batchSize);
        }
        para.put("source", source);

        dest.put("index", destIndex);
        dest.put("version_type", "internal");
        dest.put("routing", "=cat");
        para.put("dest", dest);
        setRefreshInterval(destIndex, "30s");
        NStringEntity entity = new NStringEntity(para.toJSONString(), ContentType.APPLICATION_JSON);
        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic reindex fail!");
        } finally {
            setRefreshInterval(sourceIndex, "1s");
        }
    }

    private void setRefreshInterval(String index, Object interval) {

        initClient();
        String endpoint ="/" + index + "/_settings";
        JSONObject para = new JSONObject();
        para.put("refresh_interval", interval);

        NStringEntity entity = new NStringEntity(para.toJSONString(), ContentType.APPLICATION_JSON);
        try {
            client.performRequest("PUT", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic set refresh_interval fail!");
        }
    }

    public void addAlias(String index, String ... aliases) {
        actionAlias(index, "add", aliases);
    }

    public void removeAlias(String index, String ... aliases) {
        actionAlias(index, "remove", aliases);
    }

    private void actionAlias(String index, String action, String... aliases) {

        initClient();
        JSONArray actions = new JSONArray();

        Arrays.stream(aliases).forEach(s -> {

            JSONObject obj = new JSONObject();
            obj.put("index", index);
            obj.put("alias", s);
            JSONObject actionObj = new JSONObject();
            actionObj.put(action, obj);

            actions.add(actionObj);
        });

        JSONObject paraData = new JSONObject();
        paraData.put("actions", actions);

        String endpoint ="/_aliases";
        NStringEntity entity = new NStringEntity(paraData.toJSONString(), ContentType.APPLICATION_JSON);
        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic " + action + " alias fail!");
        }
    }

    public void deleteById(String index, String type, String id) {

        initClient();
        String endpoint = "/" + index + "/" + type + "/" + id;

        try {
            client.performRequest("DELETE", endpoint, Collections.emptyMap());
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic delete doc fail!");
        }
    }

    public void deleteByQuery(String index, String type, String queryStr) {

        initClient();
        JSONObject paraData = new JSONObject();
        paraData.put("query", JSONObject.parseObject(queryStr));

        String endpoint = StringUtils.isBlank(type) ? "/" + index : "/" + index + "/" + type;
        endpoint += "/_delete_by_query?refresh&conflicts=proceed";

        NStringEntity entity = new NStringEntity(paraData.toJSONString(), ContentType.APPLICATION_JSON);

        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic delete_by_query fail!");
        }
    }

    /**
     * remove all data of index
     **/
    public void clearIndex(String index, String type) {

        String query = "{\"match_all\":{}}";
        deleteByQuery(index, type, query);
    }

    public void upsertById(String index, String type, String id, JSONObject doc) {

        initClient();
        String endpoint = "/" + index + "/" + type + "/" + id;

        NStringEntity entity = new NStringEntity(doc.toJSONString(), ContentType.APPLICATION_JSON);

        try {
            client.performRequest("PUT", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic index doc fail!");
        }
    }

    public void updateById(String index, String type, String id, JSONObject doc) {

        initClient();
        String endpoint = "/" + index + "/" + type + "/" + id + "/_update";

        doc.remove("_id");
        JSONObject paraData = new JSONObject();
        paraData.put("doc", doc);

        NStringEntity entity = new NStringEntity(paraData.toJSONString(), ContentType.APPLICATION_JSON);

        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic update doc fail!");
        }
    }

    public JSONObject findById(String index, String type, String id) {

        initClient();
        JSONObject doc = null;
        String endpoint = "/" + index + "/" + type + "/" + id;

        try {
            Response response = client.performRequest("GET", endpoint);
            String str = EntityUtils.toString(response.getEntity(), "utf-8");
            JSONObject rs = JSONObject.parseObject(str);
            doc = rs.getJSONObject("_source");
            doc.put("_id", rs.getString("_id"));
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic index doc fail!");
        }

        return doc;
    }

    public void insert(String index, String type, String jsonData) {

        initClient();
        String endpoint = "/" + index + "/" + type;

        NStringEntity entity = new NStringEntity(jsonData, ContentType.APPLICATION_JSON);

        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic insert doc fail!");
        }
    }

    public void bulkInsert(String index, String type, Collection<Map<String, Object>> dataLs) {

        initClient();
        if (dataLs.isEmpty()) {
            return;
        }
        StringBuilder builder = new StringBuilder();
        dataLs.forEach(s -> {
            Object id = s.get("_id");
            if (id != null) {
                s.remove("_id");
            } else {
                id = Snowflake.nextId();
            }
            builder.append("{\"create\":{\"_index\":\"").append(index).append("\",\"_type\":\"").append(type).append("\",\"_id\":\"").append(id).append("\" } }\n");
            builder.append(JacksonUtil.writeValueAsString(s)).append("\n");
            s.put("_id", id.toString());
        });

        String endpoint = "/_bulk";
        NStringEntity entity = new NStringEntity(builder.toString(), ContentType.APPLICATION_JSON);

        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic bulk create fail!");
        }
    }

    /**
     * 根据id批量插入更新
     * @author xiezhenxiang 2019/9/9
     **/
    public void bulkUpsertById(String index, String type, Collection<JSONObject> ls) {

        initClient();
        if (ls.isEmpty()) {
            return;
        }
        StringBuffer buffer = new StringBuffer();
        ls.forEach(s -> {

            String id = s.getString("_id");
            if (id != null) {
                buffer.append("{\"index\":{\"_index\":\"" + index + "\",\"_type\":\"" + type + "\",\"_id\":\"" + id + "\"}}\n");
                s.remove("_id");
                buffer.append(s.toJSONString() + "\n");
            }
        });

        String endpoint = "/_bulk";

        NStringEntity entity = new NStringEntity(buffer.toString(), ContentType.APPLICATION_JSON);

        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic bulk index fail!");
        }
    }

    /**
     * 根据id批量插入更新
     * @author xiezhenxiang 2019/9/9
     **/
    public void bulkDeleteById(String index, String type, Collection<String> ids) {

        initClient();
        if (ids.isEmpty()) {
            return;
        }
        StringBuffer buffer = new StringBuffer();
        ids.forEach(s -> {
            buffer.append("{\"delete\":{\"_index\":\"").append(index).append("\",\"_type\":\"").append(type).append("\",\"_id\":\"").append(s).append("\"}}\n");
        });

        String endpoint = "/_bulk";

        NStringEntity entity = new NStringEntity(buffer.toString(), ContentType.APPLICATION_JSON);

        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic bulk delete fail!");
        }
    }

    public void updateByQuery (String index, String type, String queryStr, JSONObject doc) {

        if (doc.isEmpty()) {
            return;
        }

        initClient();
        doc.remove("_id");
        String endpoint = StringUtils.isBlank(type) ? "/" + index : "/" + index + "/" + type;
        endpoint += "/_update_by_query?conflicts=proceed";

        JSONObject paraData = new JSONObject();
        paraData.put("query", JSONObject.parseObject(queryStr));

        JSONObject script = new JSONObject();
        script.put("lang", "painless");
        script.put("params", doc);
        StringBuffer source = new StringBuffer();
        doc.keySet().forEach(s -> {
            source.append("ctx._source."+ s +"=params."+ s +";");
        });
        script.put("inline", source.toString());

        paraData.put("script", script);

        NStringEntity entity = new NStringEntity(paraData.toJSONString(), ContentType.APPLICATION_JSON);

        try {
            client.performRequest("POST", endpoint, Collections.emptyMap(), entity);
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic update_by_query fail!");
        }
    }


    /**
     * 深度检索，建议1w数据量以下使用
     */
    public String findByQuery(String index, String type, Integer pageNo, Integer pageSize, String query, String sort) {

        initClient();
        Objects.requireNonNull(pageNo, "pageNo is null!");
        Objects.requireNonNull(pageSize, "pageSize is null!");

        String rs = null;
        JSONObject paraData = new JSONObject();
        paraData.put("from", (pageNo - 1) * pageSize);
        paraData.put("size", pageSize);
        if (StringUtils.isNotBlank(query)) {
            paraData.put("query", JSONObject.parseObject(query));
        }
        if (StringUtils.isNoneBlank(sort)) {
            paraData.put("sort", JSONArray.parseObject(query));
        }

        String endpoint = StringUtils.isNoneBlank(type) ? "/" + index + "/" + type : "/" + index;
        endpoint += "/_search";

        try {
            NStringEntity entity = new NStringEntity(paraData.toJSONString(), ContentType.APPLICATION_JSON);
            Response response = client.performRequest("POST", endpoint, Collections.emptyMap(), entity, consumerFactory);
            rs = EntityUtils.toString(response.getEntity(), "utf-8");
            rs = JSONPath.read(rs, "hits.hits").toString();
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic search docs fail!");
        }

        return rs;
    }

    public String[] findByScroll(String index, String type, String query, String sort, Integer size, String scrollId) {
        return findByScroll(index, type, query, sort, null, size, scrollId);
    }

    /**
     * 游标检索全量数据,配合循环使用，第一次传scrollId为null,第二次传返回的scrollId,直到数据读完位置（rs[1].length <= 2）
     */
    public String[] findByScroll(String index, String type, String query, String sort, List<String> sourceFields, Integer size, String scrollId) {

        initClient();
        Objects.requireNonNull(size, "size is null");
        String rs = "";
        JSONObject paraData = new JSONObject();
        String endpoint;
        if (scrollId == null) {

            if (StringUtils.isNotBlank(query)) {
                paraData.put("query", JSONObject.parseObject(query));
            }
            if (StringUtils.isNoneBlank(sort)) {
                paraData.put("sort", JSONArray.parseArray(sort));
            }
            if (sourceFields != null) {
                JSONObject include = new JSONObject();
                include.put("includes", sourceFields);
                paraData.put("_source", include);
            }
            paraData.put("size", size);
            endpoint = StringUtils.isNoneBlank(type) ? "/" + index + "/" + type : "/" + index;
            endpoint += "/_search?scroll=" + scrollIntervalTime;
        } else {
            endpoint = "/_search/scroll";
            paraData.put("scroll", scrollIntervalTime);
            paraData.put("scroll_id", scrollId);
        }

        try {
            NStringEntity entity = new NStringEntity(paraData.toJSONString(), ContentType.APPLICATION_JSON);
            Response response = client.performRequest("POST", endpoint, Collections.emptyMap(), entity, consumerFactory);
            rs = EntityUtils.toString(response.getEntity(), "utf-8");
            scrollId = JSONPath.read(rs, "_scroll_id").toString();
            rs = JSONPath.read(rs, "hits.hits").toString();

            if (rs.length() <= 2) {
                endpoint = "/_search/scroll/" + scrollId;
                client.performRequest("DELETE", endpoint);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
            exit("elastic search by scroll fail!");
        }

        return new String[] {scrollId, rs};
    }

    public boolean exists(String index, String type) {

        initClient();
        String endpoint = "/" + index;
        if (StringUtils.isNotBlank(type)) {
            endpoint += "/_mapping/" + type;
        }
        boolean success = false;
        try {
            Response response = client.performRequest("HEAD", endpoint);
            success = response.getStatusLine().getStatusCode() == 200;
        } catch (IOException e) {
            log.error("elastic send head index fail!");
            throw new RuntimeException(e);
        }
        return success;
    }



    public static void main(String[] args) {

        EsRestUtil restClient = getInstance("192.169.4.200:34913", "elastic", "root@hiekn");

        String[] rs = restClient.findByScroll("qb_file_db", "_doc", null, null, Lists.newArrayList("title"), 2, null);

        while (rs[1].length() > 2) {

            String str = rs[1];
            String scrollId = rs[0];
            System.out.println(str);
            rs = restClient.findByScroll("qb_file_db", "_doc", null, null, Lists.newArrayList("title"), 2, scrollId);
        }

    }

    private EsRestUtil(String userName, String password, HttpHost... hosts) {

        this.hosts = hosts;
        this.userName = userName;
        this.password = password;
        initClient();
    }

    private void initClient() {

        if (client == null) {
            synchronized (EsRestUtil.class){
                if (client == null) {
                    String key = hostStr();
                    if (pool.containsKey(key)) {
                        client = pool.get(key);
                        if (client == null) {
                            pool.remove(key);
                        } else {
                            return;
                        }
                    }

                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
                    client = RestClient.builder(hosts)
                        .setMaxRetryTimeoutMillis(1000 * 60)
                        .setRequestConfigCallback(request -> {
                            request.setConnectTimeout(1000 * 5);
                            request.setConnectionRequestTimeout(1000 * 5);
                            request.setSocketTimeout(1000 * 60 * 5);
                            return request;
                        })
                        .setHttpClientConfigCallback(s ->
                            s.setDefaultCredentialsProvider(credentialsProvider)
                            .setDefaultIOReactorConfig(IOReactorConfig.custom()
                            .setIoThreadCount(Runtime.getRuntime().availableProcessors())
                            .setConnectTimeout(1000 * 5)
                            .setSoTimeout(1000 * 60 * 5)
                            .build())
                        )
                        .build();
                    pool.put(key, client);
                }
            }
        }
    }

    private static void exit(String msg) {

        System.out.println(msg);
        System.exit(1);
    }

    private String hostStr() {

        String str = "";

        for (int i = 0; i < hosts.length; i ++) {
            str += hosts[i].toHostString() + "_";
        }
        return str.substring(0, str.length() - 1);
    }

    public RestClient getClient() {

        initClient();
        return client;
    }

    public void close () {

        if (client != null) {
            try {
                client.close();
                client = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
