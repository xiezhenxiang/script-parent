package ai.plantdata.script.util.bean;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Data
public class AttrDefine {

    private int id;
    private String name;
    private String alias;
    private Integer type;
    private Long domain;
    private List<Long> range;
    private Integer dataType;
    private String dataExpression;
    private String dataUnit;
    private Integer isFunctional;
    private List<SideAttrDefine> extraInfoList;
    private Integer tableAlone;
    private Integer joinSeqNo;
    private String gtRangeOperator;
    private String ltRangeOperator;
    private String fuzzyOperator;
    private String gtMostOperator;
    private String ltMostOperator;
    private String gtSingleMostOperator;
    private String ltSingleMostOperator;
    private String mostOperatorUnit;
    private String editTip;
    private Integer seqNo;
    private String additionalInfo;
    private String creator;
    private String createTime;
    private String modifier;
    private String modifyTime;
    private String status;
    private Integer direction;
    private String constraints;

    /** set default value */

    public String getConstraints() {
        return StringUtils.isBlank(constraints) ? "{}" : constraints;
    }

    public Integer getIsFunctional() {
        return isFunctional == null ? 0 : isFunctional;
    }

    public Integer getDirection() {
        return direction == null ? 0 : direction;
    }

    public Integer getDataType() {
        return dataType == null ? 0 : dataType;
    }

    public List<SideAttrDefine> getExtraInfoList() {
        return extraInfoList == null ? new ArrayList<>() : extraInfoList;
    }
}
