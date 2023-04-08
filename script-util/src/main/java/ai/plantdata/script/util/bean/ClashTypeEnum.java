package ai.plantdata.script.util.bean;

/**
 * 冲突类型
 * @author xiezhenxiang 2019/11/14
 **/
public enum ClashTypeEnum {

    /** 范围约束 */
    RANG("range"),
    /** 枚举约束 */
    ENUM("enum"),
    /** 数据类型不匹配 */
    DATATYPE("dataType"),
    /** 关系唯一性 */
    RELATION_ONLY("relation_only");

    private String type;

    ClashTypeEnum(String type) {
        this.type = type;
    }
    public String getType() {
        return type;
    }
}
