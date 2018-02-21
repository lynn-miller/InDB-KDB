package KDB_inDB;

import java.sql.SQLException;
import oracle.sql.STRUCT;

/**
 * Class to hold dimension details
 * 
 * @author lynnm
 */
public class Dimension {
    private String tableName;
    private String pkeyName;
    private String fkeyName;
    private boolean keyIsAttr;
    private int numAttrs;
    private int firstAttrNum;
    private int fkeyAttrNum;
    private int pkeyAttrNum;
    
    public Dimension(STRUCT dimRec) throws SQLException {
        Object[] attribs = dimRec.getAttributes();
        pkeyName = ((String)(attribs[0])).toUpperCase();
        fkeyName = ((String)(attribs[1])).toUpperCase();
        tableName = ((String)(attribs[2])).toUpperCase();
        keyIsAttr = ((String)(attribs[3])).toUpperCase().equals("Y");
    }
    
    public String getTableName() { return tableName; }
    public String getPkeyName() { return pkeyName; }
    public String getFkeyName() { return fkeyName; }
    public boolean getKeyIsAttr() { return keyIsAttr; }
    public int getNumAttrs() { return numAttrs; }
    public void setNumAttrs(int numAttrs) { this.numAttrs = numAttrs; }
    public int getFirstAttrNum() { return firstAttrNum; }
    public void setFirstAttrNum(int firstAttrNum) { this.firstAttrNum = firstAttrNum; }
    public int getLastAttrNum() { return firstAttrNum + numAttrs - 1; }
    public int getFkeyAttrNum() { return fkeyAttrNum; }
    public void setFkeyAttrNum(int fkeyAttrNum) { this.fkeyAttrNum = fkeyAttrNum; }
    public int getPkeyAttrNum() { return pkeyAttrNum; }
    public void setPkeyAttrNum(int pkeyAttrNum) { this.pkeyAttrNum = pkeyAttrNum; }
}
