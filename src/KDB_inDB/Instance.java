
package KDB_inDB;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Converts a record into attribute and value numbers
 * 
 * @author lynnm
 */
public class Instance {
    private int[] valueIndex;
    private int labelIndex;
    private int numAttributes;

    /**
     * Constructor using string array of values; attributes must be in order
     * 
     * @param nextLine   - array of attributes
     * @param attValues  - list of values for each attribute
     * @param labelIndex - indicates the class
     */
    public Instance(String[] nextLine, ArrayList[] attValues, int labelIndex) {
        this.labelIndex = labelIndex;
        this.numAttributes = attValues.length;
        valueIndex = new int[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            valueIndex[i] = attValues[i].indexOf(nextLine[i]);
        }
    }     

    /**
     * Constructor using string array of values; attributes can be unordered
     * 
     * @param nextLine   - array of attributes
     * @param attrOrder  - order of attributes in attValues
     * @param attValues  - list of values for each attribute
     * @param labelIndex - indicates the class
     */
    public Instance(String[] nextLine, int[] attrOrder, ArrayList[] attValues, int labelIndex) {
        this.labelIndex = labelIndex;
        this.numAttributes = attValues.length;
        valueIndex = new int[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            if (attrOrder[i] == -1) valueIndex[i] = -1;
            else valueIndex[i] = attValues[i].indexOf(nextLine[attrOrder[i]]);
        }
    }     

    /**
     * Constructor using Oracle result set; optionally adds any new values found
     * 
     * @param rset       - Oracle result set, pointing to record to process
     * @param attValues  - list of values for each attribute
     * @param labelIndex - indicates the class
     * @param addValues  - flag to indicate if missing values should be added to attValues
     * @param nullChar   - character used to represent NULLS
     * @throws SQLException 
     */
    public Instance(ResultSet rset, ArrayList[] attValues, int labelIndex, boolean addValues, String nullChar) throws SQLException {
        String theValue;
        this.labelIndex = labelIndex;
        this.numAttributes = attValues.length;
        valueIndex = new int[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++) {
            theValue = rset.getString(i + 1);
            if (theValue == null) theValue = nullChar;
            valueIndex[i] = attValues[i].indexOf(theValue);
            if (valueIndex[i] == -1 && addValues) {
                valueIndex[i] = attValues[i].size();
                attValues[i].add(theValue);
            }
        }
    }     

    public int classValue() {
        return valueIndex[labelIndex];
    }

    public int value(int i) {
        return valueIndex[i];
    }

    public int classIndex() {
        return labelIndex;
    }

    @Override
    public String toString() {
        String result = "Value Indexes: ";
        for (int i=0; i<valueIndex.length; i++) {
            result = result + String.format("%d, ", valueIndex[i]);
        }
        return result;
    }
    
}
