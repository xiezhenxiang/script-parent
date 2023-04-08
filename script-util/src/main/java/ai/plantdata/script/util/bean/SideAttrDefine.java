package ai.plantdata.script.util.bean;

import lombok.Data;

import java.util.List;

@Data
public class SideAttrDefine {

    private static final long serialVersionUID = -3383785867548196236L;
    private Integer seqNo;
    private String name;
    private Integer dataType;
    private Integer type;
    private List<Long> objRange;
    private String dataUnit;
    private Integer indexed;
}
