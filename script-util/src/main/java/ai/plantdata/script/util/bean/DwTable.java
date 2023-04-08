package ai.plantdata.script.util.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Date;

/**
 * @author xiezhenxiang 2019/12/25
 */
@Data
public class DwTable {

    /** 数仓ID */
    @JsonProperty("dw_database_id")
    public Long databaseId;
    /** 数仓标题 */
    public String databaseTitle;
    /** 数据表ID */
    public Long id;
    /** 数据表标题 */
    public String title;
    /** 存储介质 1-mongo 2-elasticsearch 3-mysql */
    @JsonProperty("data_type")
    public Integer dataType;
    /** 数据类型 1行业标准 2pddoc 3自定义 */
    @JsonProperty("data_format")
    public Integer dataFormat;
    public String addr;
    public String username;
    public String password;
    @JsonProperty("db_name")
    public String dbName;
    @JsonProperty("user_id")
    public String userId;
    /** 数仓创建方式 1云端 2本地 */
    @JsonProperty("create_way")
    public Integer createWay;
    /** 数据库标识 */
    @JsonProperty("data_name")
    public String dataName;
    /** 数据表唯一标识 */
    @JsonProperty("table_name")
    public String tableName;
    /** 数据表名称 */
    @JsonProperty("tb_name")
    public String tbName;
    public String fields;
    @JsonProperty("schema_config")
    public String schemaConfig;
    public String mapper;

    @JsonProperty("create_at")
    public Date createAt;
    @JsonProperty("update_at")
    public Date updateAt;
}
