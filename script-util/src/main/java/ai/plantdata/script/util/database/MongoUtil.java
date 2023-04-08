package ai.plantdata.script.util.database;

import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ai.plantdata.script.util.other.AlgorithmUtil.elfHash;

/**
 * @author xiezhenxiang 2019/4/17
 */
public class MongoUtil {


    private final String ip;
    private final List<ServerAddress> urlList;
    private final String userName;
    private final String password;
    private volatile MongoClient client;
    private final Map<Integer, MongoClient> pool = new HashMap<>(10);

    private static final Integer BATCH_SIZE = 10000;
    public static final CodecRegistry CODE_REGISTRY = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    private MongoUtil(String ip, Integer port, String userName, String password) {

        this.ip = ip;
        String[] ips = ip.split(",");
        urlList = new ArrayList<>();
        for (String one : ips) {
            urlList.add(new ServerAddress(one, port));
        }
        this.userName = userName;
        this.password = password;
        initClient();
    }

    public static MongoUtil getInstance(String ip, Integer port, String userName, String password) {

        return new MongoUtil(ip, port, userName, password);
    }

    public static MongoUtil getInstance(String hosts, String userName, String password) {

        return new MongoUtil(hosts, userName, password);
    }

    private MongoUtil(String hosts, String userName, String password) {

        StringBuilder ipStr = new StringBuilder();
        urlList = new ArrayList<>();
        String[] hostArr = hosts.split(",");
        for (String one : hostArr) {
            String[] ipPort = one.split(":");
            ipStr.append(ipPort[0]).append(",");
            urlList.add(new ServerAddress(ipPort[0], Integer.parseInt(ipPort[1])));
        }
        this.ip = ipStr.substring(0, ipStr.length() - 1);
        this.userName = userName;
        this.password = password;
        initClient();
    }

    public MongoCursor<Document> find(String db, String col, Bson query, Bson sort) {

        return find(db, col, query, sort, null, null);
    }

    public  MongoCursor<Document> find(String db, String col, Bson query) {

        return find(db, col, query, null, null, null);
    }

    public MongoCursor<Document> find(String db, String col, Bson query, Bson sort, Integer pageNo, Integer pageSize) {

        initClient();
        MongoCursor<Document> mongoCursor = null;
        query = query == null ? new Document() : query;
        sort = sort == null ? new Document() : sort;

        FindIterable<Document> findIterable = client.getDatabase(db).getCollection(col).find(query).sort(sort);
        if(pageNo != null) {
            pageNo = (pageNo - 1) * pageSize;
            findIterable.skip(pageNo);
        }
        if (pageSize != null) {
            findIterable.limit(pageSize);
        }
        mongoCursor = findIterable.batchSize(BATCH_SIZE).maxAwaitTime(10L, TimeUnit.MINUTES).iterator();
        return mongoCursor;
    }

    public MongoCursor<Document> aggregate(String db, String col, List<Bson> aggLs) {

        initClient();
        return client.getDatabase(db).getCollection(col).aggregate(aggLs).allowDiskUse(true).batchSize(BATCH_SIZE)
                .maxTime(15L, TimeUnit.MINUTES).cursor();
    }

    public void insertMany(String database, String collection, List<Document> documentList) {

        initClient();
        if (documentList == null || documentList.isEmpty()) {
            return;
        }
        InsertManyOptions insertManyOptions = new InsertManyOptions();
        insertManyOptions.ordered(false);
        try {
            client.getDatabase(database).getCollection(collection).insertMany(documentList, insertManyOptions);
        } catch (Exception e) {
            // 唯一约束的skip, 不然插入会丢数据
            if (!e.getMessage().contains("duplicate")) {
                throw new RuntimeException(e);
            }
        }
    }

    public void insertOne(String database, String collection, Document doc) {

        initClient();
        client.getDatabase(database).getCollection(collection).insertOne(doc);
    }

    public void updateOne(String database, String collection, Bson query, Document doc) {

        initClient();
        client.getDatabase(database).getCollection(collection).updateOne(query, new Document("$set", doc));
    }

    public void upsertOne(String database, String collection, Bson query, Document doc) {

        initClient();
        client.getDatabase(database).getCollection(collection).replaceOne(query, doc, new UpdateOptions().upsert(true));
    }

    public void push(String database, String collection, Bson query, String field, Object... value) {

        initClient();
        Document pushDoc = new Document("$push", new Document(field, new Document("$each", Lists.newArrayList(value))));
        client.getDatabase(database).getCollection(collection).updateMany(query, pushDoc);
    }

    public void upsertMany(String database, String collection, Collection<Document> ls, boolean upsert, String... fieldArr) {

        initClient();
        if (ls == null || ls.isEmpty()) {
            return;
        }

        List<UpdateManyModel<Document>> requests = ls.stream().map(s -> new UpdateManyModel<Document>(
                new Bson() {
                    @Override
                    public <TDocument> BsonDocument toBsonDocument(Class<TDocument> aClass, CodecRegistry codecRegistry) {
                        Document doc = new Document();
                        for (String field : fieldArr) {
                            doc.append(field, s.get(field));
                        }
                        return doc.toBsonDocument(aClass, codecRegistry);
                    }
                },
                new Document("$set",s),
                new UpdateOptions().upsert(upsert)
        )).collect(Collectors.toList());

        client.getDatabase(database).getCollection(collection).bulkWrite(requests);
    }

    public void updateMany(String database, String collection, Bson query, Document doc) {

        initClient();
        client.getDatabase(database).getCollection(collection).updateMany(query, new Document("$set", doc));
    }

    public Long count(String db, String col, Bson query){

        initClient();
        return client.getDatabase(db).getCollection(col).count(query);
    }

    public List<Document> getIndex(String db, String col) {

        initClient();
        List<Document> indexLs = new ArrayList<>();
        MongoCursor<Document> cursor = client.getDatabase(db).getCollection(col).listIndexes().iterator();
        cursor.forEachRemaining(s -> indexLs.add((Document) s.get("key")));

        return indexLs;
    }

    public void delete(String db, String col, Bson query){

        initClient();
        client.getDatabase(db).getCollection(col).deleteMany(query);
    }

    public void createIndex(String db, String col, Document... indexArr) {

        initClient();
        for (Document index : indexArr) {
            client.getDatabase(db).getCollection(col).createIndex(index);
        }
    }

    public void dropIndex(String db, String col, Document... indexArr) {

        initClient();
        for (Document index : indexArr) {
            client.getDatabase(db).getCollection(col).dropIndex(index);
        }
    }


    public synchronized void copyDataBase(String fromDbName, String toDbName) {

        initClient();
        MongoIterable<String> colNames = client.getDatabase(fromDbName).listCollectionNames();

        for (String colName : colNames) {
            copyCollection(fromDbName, colName, toDbName, colName);
        }
    }

    public void copyCollection(String fromDbName, String fromColName, String toDbName, String toColName) {

        initClient();
        copyCollection(this, fromDbName, fromColName, this, toDbName, toColName);
    }

    public void copyCollection(String fromDbName, String fromColName, MongoUtil toMongoUtil, String toDbName, String toColName) {

        initClient();
        copyCollection(this, fromDbName, fromColName, toMongoUtil, toDbName, toColName);
    }

    private void copyCollection(MongoUtil fromMongo, String fromDbName, String fromColName, MongoUtil toMongo, String toDbName, String toColName) {

        initClient();
        List<Document> indexLs = fromMongo.getIndex(fromDbName, fromColName);

        toMongo.createIndex(toDbName, toColName,  indexLs.toArray(new Document[0]));

        MongoCursor<Document> cursor = fromMongo.find(fromDbName, fromColName, null);

        List<Document> docLs = new ArrayList<>();

        cursor.forEachRemaining(doc -> {

            docLs.add(doc);
            if (docLs.size() >= BATCH_SIZE) {
                toMongo.insertMany(toDbName, toColName, docLs);
                docLs.clear();
            }
        });

        toMongo.insertMany(toDbName, toColName, docLs);
    }

    public static void main(String[] args) {
        MongoUtil mongoUtil = getInstance("192.168.4.135:19130", "", "");
        System.out.println(mongoUtil.count("bj735trh_graph_17250062acd", "basic_info", null));
    }

    private void initClient() {

        if (client == null) {

            synchronized (MongoUtil.class){
                if (client == null) {
                    Integer key = elfHash(ip);
                    if (pool.containsKey(key)) {
                        client = pool.get(key);
                        if (client == null) {
                            initClient();
                        }
                    } else {
                        try {
                            MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(userName, "admin", password.toCharArray());
                            MongoClientOptions options = MongoClientOptions.builder()
                                    .connectionsPerHost(400)
                                    .minConnectionsPerHost(1)
                                    .maxConnectionIdleTime(0)
                                    .maxConnectionLifeTime(0)
                                    .connectTimeout(3000)
                                    .maxWaitTime(1000 * 60 * 10)
                                    .socketTimeout(0)
                                    .codecRegistry(CODE_REGISTRY)
                                    .build();
                            if (StringUtils.isNoneBlank(userName, password)) {
                                client = new MongoClient(urlList, mongoCredential, options);
                            } else {
                                client = new MongoClient(urlList, options);
                            }
                            pool.put(key, client);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("mongo connect error!");
                            System.exit(1);
                        }
                    }
                }
            }
        }
    }

    public MongoClient getClient() {
        initClient();
        return client;
    }
}
