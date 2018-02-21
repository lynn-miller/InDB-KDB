package KDB_inDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleStatement;

/**
 * Predicts the class for the records returned by a query using an in-database KDB model
 * 
 * @author Lynn Miller
 * 
 */
public class KdbPredict {
    
    // Database connection parameters - not used with in-database JVM
//    private static String dbConnStr = "localhost:1521:XE";
    private static String dbConnStr = "rhino.its.monash.edu.au:1521:EDUDB";
    private static String dbUser = "bayes";
    private static String dbPassword = "";
    
    private static String pQuery = "select * from nursery_input where rownum <= 100";
    private static String pModel = "unnamed";
    private static String pRowid = null;
    
    private static OracleConnection conn;
    
    private static OracleConnection openConnection() throws SQLException {
        OracleConnection conn;
        DriverManager.registerDriver (new oracle.jdbc.OracleDriver());
        if (System.getProperty("oracle.jserver.version") == null) {
            // Out of database JVM - connect to the database
            conn = (OracleConnection)DriverManager.getConnection("jdbc:oracle:thin:@" + dbConnStr, dbUser, dbPassword);
            conn.setAutoCommit(false);
            System.out.println("Connected to " + dbConnStr + " database");
        } else {
            // In database JVM - retrieve default connection
            conn = (OracleConnection)(new OracleDriver()).defaultConnection();
        }
        return conn;
    }
    
    private static void closeConnection(Connection conn) throws SQLException {
        // If out of database JVM close the connection, otherwise do nothing
        if (System.getProperty("oracle.jserver.version") == null) {
            conn.close();
            System.out.println("Closed " + dbConnStr + " connection");
        }
    }

/**
 * Returns a result set containing the class probabilities for each required prediction
 * 
 * @param  pQuery An SQL query returning a set of rows needing classification
 * @param  pModel The name of the KDB model to use as the classifier
 * @param  pRowid A column returned by pQuery to use to identify the query rows
 *                Defaults to the Oracle rownum column
 * @return        An Oracle result set containing the rowid, class and class probability
 *                for each combination of class and record
 */
    public static OracleResultSet predict (String pQuery, String pModel, String pRowid) throws SQLException {
        String query;
        String structure = "KDB$S_" + pModel.toUpperCase();
        String modelName = "KDB$M_" + pModel.toUpperCase();
        String caseBody = "";
        String rowid = pRowid == null ? "rownum" : pRowid;
        OraclePreparedStatement selectStmt;
        OracleResultSet rset;
        
        conn = openConnection();
        
        // Get the list of attributes, format into string of case "when" clauses
        query = "select distinct kdb$attr_name from " + structure + " where kdb$mi_rank is not null";
        selectStmt = (OraclePreparedStatement)conn.prepareStatement(query);
        rset = (OracleResultSet)selectStmt.executeQuery();
        while (rset.next()) {
            caseBody = caseBody + " when '" + rset.getString(1) + "' then " + rset.getString(1);
        }
        rset.close();
        
        // Get K, the maximum number of parents
        query = "select nvl(max(kdb$cmi_rank),0) K from " + structure;
        selectStmt = (OraclePreparedStatement)conn.prepareStatement(query);
        rset = (OracleResultSet)selectStmt.executeQuery();
        rset.next();
        int K = rset.getInt(1);
        rset.close();
        
        // Construct the prediction query
        query = "select kdb$rowid, kdb$class, " +
                       "exp(kdb$log_prob - (ln(greatest(sum(exp(kdb$log_prob-kdb$max_log)) over (partition by kdb$rowid),1e-75)) + kdb$max_log)) kdb$prob " +
                "from ( select kdb$rowid, kdb$class, sum(kdb$log_prob) kdb$log_prob, max(sum(kdb$log_prob)) over (partition by kdb$rowid) kdb$max_log " +
                       "from ( select bn.* " +
                              "from ( select p.kdb$rowid, m.*, " +
                                            "max(kdb$group_id) over(partition by p.kdb$rowid, m.kdb$attr_name) kdb$max_group, " +
                                            "ln((kdb$count+1/kdb$num_values)/(kdb$parents_count+1)) kdb$log_prob " +
                                     "from ( select kdb$rowid, kdb$attr_name, to_char(case kdb$attr_name " + caseBody + " end) kdb$attr_value";
        for (int k = 1; k <= K; k++) {
            // Add in each parent
            query = query + ", to_char(case kdb$parent" + k + " " + caseBody + " end) kdb$parent" + k;
        }
        query = query + " from ( select kdb$attr_name";
        for (int k = 1; k <= K; k++) {
            query = query + ", kdb$parent" + k;
        }
        query = query + " from " + structure + " ";
        if (K > 0) {
            query = query + "pivot (max(kdb$attr_name2) for kdb$cmi_rank in (";
            String sepr = "";
            for (int k = 1; k <= K; k++) {
                query = query + sepr + k + " kdb$parent" + k;
                sepr = ", ";
            }
            query = query + ")) ";
        }
        query = query + "where kdb$mi_rank is not null ), " +
                "( select " + rowid + " kdb$rowid, p1.* from (" + pQuery + ") p1 ) " +
                ") p, " + modelName + " m " +
                "where p.kdb$attr_name = m.kdb$attr_name " +
                  "and p.kdb$attr_value = m.kdb$attr_value ";
        for (int k = 1; k <= K; k++) {
            // join condition for each parent - nulls in the model match anything
            query = query + "and nvl(p.kdb$parent" + k + ", '~') = coalesce(m.kdb$parent" + k + ", p.kdb$parent" + k + ", '~') ";
        }
        query = query + " ) bn where kdb$group_id = kdb$max_group ) " +
                "group by kdb$rowid, kdb$class )";
        
        // Run the prediction query
        System.out.println(query);
        conn.setCreateStatementAsRefCursor(true);
        OracleStatement stmt = (OracleStatement)conn.createStatement();
        return (OracleResultSet)stmt.executeQuery(query);
    }

/**
 * Creates prediction objects for each record and returns them in an ArrayList
 * 
 * @param  rset An oracle result set containing the class probabilities
 * @return      An ArrayList of prediction objects
 */
    public static ArrayList<Prediction> getPredictions (ResultSet rset) throws SQLException {
        ArrayList<Prediction> predictions = new ArrayList<Prediction>();
        // Retrieve the results
        int oldRowid = -9999999;
        Prediction thisPrediction = null;
        while (rset.next()) {
            int thisRowid = rset.getInt(1);
            // Is this a new prediction?
            if (thisPrediction == null || thisRowid != oldRowid) {
                // Yes - add current prediction to results and start creating a new prediction
                oldRowid = thisRowid;
                if (thisPrediction != null) predictions.add(thisPrediction);
                thisPrediction = new Prediction(thisRowid, rset.getString(2), rset.getDouble(3));      
            } else {
                // No - extract the class and probabilities and add to current prediction
                thisPrediction.addClassProb(rset.getString(2), rset.getDouble(3));
            }
        }
        // add last prediction to results
        if (thisPrediction != null) predictions.add(thisPrediction);
        rset.close();
        closeConnection(conn);
        return predictions;
    }

    private static void setOptions(String[] args) {
        String option = "";
        String thisParam = "";
        for (int i=0; i < args.length; i++) {
            thisParam = args[i];
            if (thisParam.substring(0,1).equals("-")) thisParam = args[i].substring(1);
            option = thisParam.substring(0,1);
            if (thisParam.length() == 1) {
                if (i+1 < args.length) thisParam = args[i+1];
            } else if (thisParam.substring(1,2).equals("=")) {
                thisParam = thisParam.substring(2);
            } else {
                thisParam = thisParam.substring(1);
            }
            if (option.equals("D")) {
                dbConnStr = thisParam;
            } else if (option.equals("U")) {
                dbUser = thisParam;
            } else if (option.equals("P")) {
                dbPassword = thisParam;
            } else if (option.equals("M")) {
                pModel = thisParam;
            } else if (option.equals("Q")) {
                pQuery = thisParam;
            } else if (option.equals("R")) {
                pRowid = thisParam;
            }
        }
        System.out.printf("M:%s;Q:%s;R:%s\n", pModel, pQuery, pRowid);
    }

/**
 * Predicts the class for the records returned by a query using an in-database
 * KDB model and prints the results
 * 
 * @param -D The database connection string - not required for Oracle JVM
 * @param -U The database user name - not required for Oracle JVM
 * @param -P The database user password - not required for Oracle JVM
 * @param -Q An SQL query returning a set of rows needing classification
 * @param -M The name of the KDB model to use as the classifier
 * @param -R A column returned by pQuery to use to identify the query rows
 */
    public static void main(String[] args) {
        ArrayList<Prediction> predictions;
        setOptions(args);
        try {
            predictions = getPredictions(predict(pQuery, pModel, pRowid));
            Iterator<Prediction> predictIter = predictions.iterator();
            while (predictIter.hasNext()) {
		System.out.println(predictIter.next());
	    }

        } catch (SQLException e) {
            System.out.println("SQL error: " + e);
        }
    }
}
