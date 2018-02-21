package KDB_inDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author lynnm
 */
public class AttrValues {
    
    String[] attributes;
    ArrayList[] attValues;
    HashMap<String, Integer> attIndexes;
    int numCols;
    int numObs;
    int numClasses;
    int labelIndex;
    
    /**
     * KDB Pass 2 - create the model. Called directly when running in-database
     *
     * @param pQuery   - a query returning the training data
     * @param pLabel   - the class attribute
     * @param conn     - a database connection object - connection should be open
     * @param nullChar - value used to represent NULLs
     */
    public AttrValues(String pQuery, String pLabel, Connection conn, String nullChar) throws SQLException {

        PreparedStatement selectStmt;
        ResultSet rset;
        ResultSetMetaData mdata;
        String[] columns;
        int thisIndex;
        
        // Run the query to get the training data
        selectStmt = conn.prepareStatement(pQuery);
        selectStmt.setFetchSize(1000);
        rset = selectStmt.executeQuery();
        
        // Process the query metadata to get attribute names
        mdata = rset.getMetaData();
        numCols = mdata.getColumnCount();
        attributes = new String[numCols];
        columns = new String[numCols];
        attIndexes = new HashMap<String, Integer>();
        for (int i = 0; i < numCols; i++) {
            attributes[i] = mdata.getColumnName(i + 1);
            attIndexes.put(attributes[i], i);
            if (attributes[i].equals(pLabel)) labelIndex = i;
        }
        attValues = new ArrayList[numCols];
        for (int i = 0; i < numCols; i++) {
            attValues[i] = new ArrayList();
        }
        
        // Read each record, if new attribute values are found add to the list
        numObs = 0;
        while (rset.next()) {
            numObs++;
            for (int i = 0; i < numCols; i++) {
                columns[i] = rset.getString(i + 1);
                if (columns[i] == null) columns[i] = nullChar;
                thisIndex = attValues[i].indexOf(columns[i]);
                if ( thisIndex == -1 ) {
                    attValues[i].add(columns[i]);
                }
            } 
        }
        
        // Close result set and statement
        rset.close();
        selectStmt.close();
        numClasses = attValues[labelIndex].size();
    }
    
    public String[] getAttributes () { return attributes; }
    public ArrayList[] getAttValues () { return attValues; }
    public void setAttValues (int attIndex, ArrayList values) { attValues[attIndex] = values; }
    public int getNumCols () { return numCols; }
    public int getNumObs () { return numObs; }
    public int getNumClasses () { return numClasses; }
    public int getClassIdx () { return labelIndex; }
    public int getAttIndex (String attribute) { return attIndexes.get(attribute); }

}
