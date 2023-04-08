package ai.plantdata.script.util.database;

import com.google.common.collect.Lists;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static ai.plantdata.script.util.other.AlgorithmUtil.elfHash;


/**
 * support mysql, hive, dm
 * @author xiezhenxiang 2019/8/1
 **/
public class DriverUtil {

    private final String url;
    private String userName;
    private String pwd;
    private Connection con;
    private static final ConcurrentHashMap<Integer, Connection> pool = new ConcurrentHashMap<>(10);

    public static DriverUtil getInstance(String url, String userName, String pwd ) {

        return new DriverUtil(url, userName, pwd);
    }

    public static DriverUtil getMysqlInstance(String ip, Integer port, String database, String userName, String pwd) {

        String url = "jdbc:mysql://" + ip + ":" + port + "/"  + database
                + "?serverTimezone=UTC&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&useSSL=false";
        return new DriverUtil(url, userName, pwd);
    }

    private DriverUtil(String url, String userName, String pwd) {

        this.url = url;
        this.userName = userName;
        this.pwd = pwd;
        initConnection();
    }


    /**
     * 增删改
     * @author xiezhenxiang 2019/5/14
     * @param sql sql语句
     * @param params 参数
     **/
    public boolean update(String sql, Object... params) {

        initConnection();
        PreparedStatement statement;
        int result = 0;
        try {
            statement = con.prepareStatement(sql);
            int index = 1;
            if (params != null && params.length > 0) {
                for (Object para : params) {
                    statement.setObject(index++, para);
                }
            }
            // update lines num
            result = statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result > 0;
    }

    /**
     * 查找
     * @author xiezhenxiang 2019/5/14
     * @param sql sql语句
     * @param params 参数
     **/
    public List<Map<String, Object>> find(String sql, Object... params) {

        initConnection();
        List<Map<String, Object>> ls = new ArrayList<>();
        PreparedStatement statement;
        try {
            int index = 1;
            statement = con.prepareStatement(sql);
            if (params != null && params.length > 0) {
                for (Object para : params) {
                    statement.setObject(index ++, para);
                }
            }
            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int colsLen = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> obj = new HashMap<>();
                for (int i = 0; i < colsLen; i++) {
                    String colsName = metaData.getColumnName(i + 1);
                    Object colsValue = resultSet.getObject(colsName);
                    obj.put(colsName, colsValue);
                }
                ls.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ls;
    }

    public Map<String, Object> findOne(String sql, Object... params){

        initConnection();
        List<Map<String, Object>> ls = find(sql, params);
        return ls.isEmpty() ? new HashMap<>() : ls.get(0);
    }

    public List<String> getTables() {

        initConnection();
        List<String> ls;
        if (url.contains("jdbc:hive2")) {
            ls = find("show tables").stream().map(s -> s.get("tab_name").toString()).collect(Collectors.toList());
        } else {
            String dbName =url.substring(url.lastIndexOf("/") + 1, url.lastIndexOf("?"));
            String infoMysqlUrl = url.replaceAll(dbName, "information_schema");
            DriverUtil infoMysqlUtil = getInstance(infoMysqlUrl, userName, pwd);
            String sql = "select TABLE_NAME, TABLE_COMMENT from TABLES where TABLE_SCHEMA = ?";
            ls = infoMysqlUtil.find(sql, dbName).stream().map(s -> s.get("TABLE_NAME").toString()).collect(Collectors.toList());
        }
        return ls;
    }

    public void insertSelective(String tbName, Map<String, Object> map) {
        insertSelective(tbName, map, false);
    }

    /**
     * 插入数据
     * @author xiezhenxiang 2019/6/1
     **/
    public void insertSelective(String tbName, Map<String, Object> map, boolean upsertOnDuplicateKey) {

        String sql = "insert into " + tbName + " (";
        List<Object> values = new ArrayList<>();
        String duplicateKeySql = "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                sql += "`" + entry.getKey() + "`, ";
                values.add(entry.getValue());
                if (upsertOnDuplicateKey) {
                    duplicateKeySql += entry.getKey() + "=?, ";
                }
            }
        }
        if (!values.isEmpty()) {
            sql = sql.substring(0, sql.length() - 2) + ") values (";
            for (int i = 0; i < values.size(); i ++) {
                sql += "?, ";
            }
            sql = sql.substring(0, sql.length() - 2) + ")";
            if (upsertOnDuplicateKey) {
                sql += " on duplicate key update " + duplicateKeySql;
                sql = sql.substring(0, sql.length() - 2);
            }
        } else {
            return;
        }
        if (upsertOnDuplicateKey) {
            values.addAll(values);
        }
        update(sql, values.toArray());
    }



    public boolean updateSelective(String tbName, Map<String, Object> bean, String... queryField) {

        List<String> queryFieldLs = Lists.newArrayList(queryField);
        String sql = "update " + tbName + " set ";
        List<Object> values = new ArrayList<>();

        for (Map.Entry<String, Object> entry : bean.entrySet()) {
            if (!queryFieldLs.contains(entry.getKey()) && entry.getValue() != null) {
                sql += entry.getKey() + " = ? , ";
                values.add(entry.getValue());
            }
        }

        if (!values.isEmpty()) {

            sql = sql.substring(0, sql.length() - 2);
            if (!queryFieldLs.isEmpty()) {
                sql += " where ";
                for (String field : queryFieldLs) {
                    sql += field + " = ? and ";
                    values.add(bean.get(field));
                }

                sql = sql.substring(0, sql.length() - 5);
            }
            return update(sql, values.toArray());
        }

        return true;
    }


    private void initConnection() {

        if (con == null) {

            synchronized (DriverUtil.class) {

                if (con == null) {

                    userName = userName == null ? "" : userName;
                    pwd = pwd == null ? "" : pwd;
                    Integer key = elfHash(url + userName + pwd);

                    try {
                        if (pool.containsKey(key)) {
                            con = pool.get(key);
                            if (con == null || con.isClosed()) {
                                pool.remove(key);
                                initConnection();
                            }
                        } else {
                            String className = "com.mysql.jdbc.Driver";
                            if (url.contains("jdbc:dm:")) {
                                className = "dm.jdbc.driver.DmDriver";
                            } else if (url.contains("jdbc:hive")) {
                                className = "org.apache.hive.jdbc.HiveDriver";
                            } else if (url.contains("jdbc:oracle")) {
                                className = "oracle.jdbc.driver.OracleDriver";
                            }
                            Class.forName(className);
                            con = DriverManager.getConnection(url, userName, pwd);
                            pool.put(key, con);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException("datasource connect error!");
                    }
                }
            }
        }
    }

    public Connection getClient() {
        return con;
    }
}