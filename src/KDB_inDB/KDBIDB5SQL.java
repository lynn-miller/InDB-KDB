package KDB_inDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * In-database KDB - Pivot method
 * Pass 1 is Pivot - Uses nest un-pivot clauses to convert records into attribute pairs
 * Pass 2 not included in results (reuses PerAttr code)
 *
 * @author lynnm
 */
public class KDBIDB5SQL {

//  Connection information for OOD testing    
//    private static String m_DB = "localhost:1521:XE";
    private static String m_DB = "rhino.its.monash.edu.au:1521:EDUDB";
    private static String m_User = "bayes";
    private static String m_Password = "";

//  Default parameters
    private static int m_K = 2; //2;
    private static String m_Query = "select * from nursery_input";
    private static String m_Label = "OUTCOME";
    private static boolean m_Trace = false;
    private static String m_Model = "unnamed";

    /**
     * Only used for OOD testing
     * 
     * @param args are:
     *    K - the maximum number of parents
     *    L - the label (class) attribute
     *    M - the model name
     *    Q - the query returning the training data
     *    T - switch to enable SQL tracing
     */
    private static void setOptions(String[] args) throws Exception {
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
            if (option.equals("K")) {
                m_K = Integer.parseInt(thisParam);
            } else if (option.equals("L")) {
                m_Label = thisParam;
            } else if (option.equals("M")) {
                m_Model = thisParam;
            } else if (option.equals("Q")) {
                m_Query = thisParam;
            } else if (option.equals("T")) {
                m_Trace = true;
            }
        }
        System.out.printf("K:%d;L:%s;Q:%s\n", m_K, m_Label, m_Query);
    }

    /**
     * Retrieve the connection when using in-database JVM
     * Else connect to the database using connect string, user name and password
     */
    private static Connection openConnection() throws SQLException {
        Connection conn = null;
        if (System.getProperty ("oracle.jserver.version") == null) {
            conn = DriverManager.getConnection("jdbc:oracle:thin:@" + m_DB, m_User, m_Password);
            conn.setAutoCommit (false);
            System.out.println("Connected to " + m_DB + " database");
        } else {
            conn = DriverManager.getConnection("jdbc:default:connection:");
        }
        if (m_Trace) {
            try {
                conn.prepareCall("begin dbms_monitor.session_trace_enable(waits=>true); end;").execute();
                ResultSet rset = conn.prepareStatement("select value from v$diag_info where name = 'Default Trace File'").executeQuery();
                rset.next();
                System.out.println("Tracefile name: " + rset.getString(1));
                rset.close();
            } catch(SQLException e) {
                System.out.println("Error setting trace: " + e.getMessage());
            }
        }
        return conn;
    }
    
    /** Close the OOD database connection */
    private static void closeConnection(Connection conn) throws SQLException {
        if (System.getProperty ("oracle.jserver.version") == null) {
            conn.close();
            System.out.println("Closed " + m_DB + " connection");
        }
    }

    /** Run an SQL statement */
    private static void runSQL(String SQLStatement, Connection conn) throws SQLException {
        PreparedStatement preparedStmt = conn.prepareStatement(SQLStatement);
        preparedStmt.execute();
        preparedStmt.close();
    }

   /** Main is only used for OOD testing */
    public static void main(String[] args) {
        try {
            setOptions(args);
        } catch (Exception ex) {
            Logger.getLogger(KDBIDB3SQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            pass1Sql(m_Query, m_Label, m_K, m_Model);
            pass2Sql(m_Query, m_Label, m_K, m_Model);
//            printModel();
//            predict(100);
        } catch (SQLException e) {
            System.out.println("SQL error: " + e);
        }
    }
   
    /**
     * KDB Pass 1 - create the structure. Called directly when running in-database
     *
     * @param pQuery - a query returning the training data
     * @param pLabel - the class attribute
     * @param pK     - K - the number of parents
     * @param pModel - the model name - used in the structure table name
     */
    public static void pass1Sql (String pQuery, String pLabel, int pK, String pModel) throws SQLException {
        String structure = "KDB$S_" + pModel.toUpperCase();
        String viewName = "KDB$V_" + pModel.toUpperCase();
        String className = pLabel.toUpperCase();

        String query;
        String attrList1 = "";
        String attrList2 = ", ";
        String attrList1p = "";
        String attrList2p = "";
        String sepr = "";
        String attrName = "";
        String convert;
        String suffix = "$";
        int classIndex = 0;

        PreparedStatement selectStmt;
        ResultSet rset;
        Connection conn = openConnection();

        // Truncate the temporary tables - in case still populated
        runSQL("truncate table kdb_temp_ycounts", conn);
        runSQL("truncate table kdb_temp_xycounts", conn);
        runSQL("truncate table kdb_temp_xxycounts", conn);

        // Create a view of the query for ease of processing
        runSQL("create or replace view " + viewName + " as " + pQuery, conn);

        try {
            runSQL("drop table " + structure + " purge", conn);
        } catch (SQLException e) {}
        
        // Get the attribute names from the data dictionary
        selectStmt = conn.prepareStatement("select column_name, " +
                "    case when data_type like '%CHAR%' then 'n' else 'y' end convert " +
                "from user_tab_columns " +
                "where table_name = '" + viewName + "'" +
                "order by column_id");
        rset = selectStmt.executeQuery();
        int attrNum = 0;

        // Generate formatted attribute lists for use in the insert statements
        while (rset.next()) {
            attrNum++;
            convert = rset.getString(2);
            if (rset.getString(1).equals(className)) classIndex = attrNum;
            else {
                attrName = rset.getString(1);
                if (convert.equals("y")) {
                    attrList1 = attrList1 + sepr + "to_char(" + attrName + ") " + attrName + suffix;
                    attrList2 = attrList2 + sepr + "to_char(" + attrName + ") " + attrName;
                } else {
                    attrList1 = attrList1 + sepr + attrName + " " + attrName + suffix;
                    attrList2 = attrList2 + sepr + attrName;
                }
                attrList1p = attrList1p + sepr + attrName + suffix;
                attrList2p = attrList2p + sepr + attrName;
                sepr = ", ";
            }
        }

        if (pK == 0) {
            // If K=0, only the xy counts are needed
            query = "insert /*+ append */ into kdb_temp_xycounts " +
                    "select kdb$attr_name, kdb$attr_value, kdb$class, count(*) kdb$count, null, null " +
                    "from ( select * " +
                           "from ( select " + pLabel + " kdb$class" + attrList2 + " from ( " + pQuery + ") ) " +
                           "unpivot include nulls (kdb$attr_value for kdb$attr_name in (" + attrList2p + ")) ) " +
                    "group by kdb$attr_name, kdb$attr_value, kdb$class";
        } else {
            // Other K's need xy and xxy counts - generate both at once with multi-table insert
            query = "insert /*+ append */ first " +
                    "when kdb$num_attrs = 1 then into kdb_temp_xycounts " +
                        "values(kdb$attr_name1, kdb$attr_value1, kdb$class, kdb$count, null, null) " +
                    "else into kdb_temp_xxycounts " + 
                        "values(kdb$attr_name1, kdb$attr_value1, kdb$attr_name2, kdb$attr_value2, kdb$class, kdb$count) " +
                    "select case when rtrim(kdb$attr_name1,'" + suffix + "') = kdb$attr_name2 then 1 else 2 end kdb$num_attrs, " +
                           "rtrim(kdb$attr_name1,'" + suffix + "') kdb$attr_name1, kdb$attr_value1, " +
                           "kdb$attr_name2, kdb$attr_value2, kdb$class, count(*) kdb$count " +
                    "from ( select * " +
                           "from ( select " + pLabel + " kdb$class, " + attrList1 + attrList2 + " from ( " + pQuery + ") ) " +
                           "unpivot include nulls (kdb$attr_value1 for kdb$attr_name1 in (" + attrList1p + ")) ) " +
                    "unpivot include nulls (kdb$attr_value2 for kdb$attr_name2 in (" + attrList2p + ")) " +
                    "group by kdb$attr_name1, kdb$attr_value1, kdb$attr_name2, kdb$attr_value2, kdb$class";
        }
        
        // Run the insert
//        System.out.println(query);
        runSQL("alter system flush buffer_cache", conn);
        runSQL(query, conn);
        conn.commit();
        
        // Update information in xy counts table
        query = "update kdb_temp_xycounts a set "
                + "kdb$num_values = (select count(distinct nvl(kdb$attr_value, '~')) from kdb_temp_xycounts b "
                +                   "where a.kdb$attr_name = b.kdb$attr_name), "
                + "kdb$attr_num = (select column_id from user_tab_columns c "
                +                 "where a.kdb$attr_name = c.column_name "
                +                   "and c.table_name = '" + viewName + "')";
//        System.out.println(query);
        runSQL(query, conn);
        
        // Create the y counts from the xy counts
        query = "insert /*+ append */ into kdb_temp_ycounts "
                + "select kdb$class, sum(kdb$count) kdb$count "
                + "from kdb_temp_xycounts "
                + "where kdb$attr_name = '" + attrName + "' "
                + "group by kdb$class";
//        System.out.println(query);
        runSQL(query, conn);
        conn.commit();
        
        // Construct the query to create the structure table from temporay tables
        query = "create table kdb$s_" + pModel + " pctfree 0 as " +
           "with y_counts as ( " +
               "select kdb$class, kdb$count, sum(kdb$count) over () kdb$n from kdb_temp_ycounts" +
               "), " +
           "mi as ( " +
               "select rank() over (order by sum(kdb$mi) desc, min(rownum)) kdb$mi_rank, " + 
                      "kdb$attr_name, sum(kdb$mi) kdb$mi, kdb$attr_num, kdb$num_values " +
               "from ( select xy_counts.kdb$attr_name, xy_counts.kdb$attr_num, xy_counts.kdb$num_values, " +
                             "(xy_counts.kdb$count / kdb$n) * log(2,xy_counts.kdb$count/(av_count/kdb$n * y_counts.kdb$count)) kdb$mi " +
                      "from kdb_temp_xycounts xy_counts, " +
                           "( select kdb$attr_name, kdb$attr_value, sum(kdb$count) av_count  " +
                             "from kdb_temp_xycounts xy_counts  " +
                             "group by kdb$attr_name, kdb$attr_value " +
                           ") av_counts, " +
                           "y_counts " +
                      "where xy_counts.kdb$attr_name = av_counts.kdb$attr_name " +
                        "and xy_counts.kdb$attr_value = av_counts.kdb$attr_value " +
                        "and xy_counts.kdb$class = y_counts.kdb$class " +
                   " ) " +
               "group by kdb$attr_name, kdb$attr_num, kdb$num_values " +
               "), " +
           "cmi as ( " +
               "select kdb$attr_name1, kdb$attr_name2, sum(kdb$cmi) kdb$cmi " +
               "from ( select a.kdb$attr_name1, a.kdb$attr_name2, " +
                             "(a.kdb$count/b.kdb$n) * log(2,(a.kdb$count*b.kdb$count) / (d.kdb$count*e.kdb$count)) kdb$cmi " +
                      "from kdb_temp_xycounts d, kdb_temp_xycounts e, y_counts b, " +
                           "( select kdb$attr_name1, kdb$attr_value1, kdb$attr_name2, " +
                                   " kdb$attr_value2, kdb$class, kdb$count " +
                             "from kdb_temp_xxycounts " +
                           ") a " +
                      "where a.kdb$class = b.kdb$class " +
                        "and a.kdb$attr_name1 = d.kdb$attr_name " +
                        "and a.kdb$attr_value1 = d.kdb$attr_value " +
                        "and a.kdb$class = d.kdb$class " +
                        "and a.kdb$attr_name2 = e.kdb$attr_name " +
                        "and a.kdb$attr_value2 = e.kdb$attr_value " +
                        "and a.kdb$class = e.kdb$class " +
                   " ) " +
                   " group by kdb$attr_name1, kdb$attr_name2  " +
               ") " +
           "select kdb$attr_name1 kdb$attr_name, kdb$attr_name2, kdb$mi_rank, kdb$cmi_rank, kdb$attr_num, kdb$num_values " +
           "from ( select cmi.kdb$attr_name1, cmi.kdb$attr_name2, mi1.kdb$mi_rank, mi1.kdb$mi, cmi.kdb$cmi, " +
                         "rank() over (partition by cmi.kdb$attr_name1 order by cmi.kdb$cmi desc, rownum) kdb$cmi_rank, " +
                         "mi1.kdb$attr_num, mi1.kdb$num_values " +
                  "from cmi, mi mi1, mi mi2 " +
                  "where cmi.kdb$attr_name1 = mi1.kdb$attr_name " +
                    "and cmi.kdb$attr_name2 = mi2.kdb$attr_name " +
                    "and mi1.kdb$mi_rank >= mi2.kdb$mi_rank ) a " +
           "where a.kdb$cmi_rank <= " + pK + " " +
           "union all " +
           "select kdb$attr_name, null, kdb$mi_rank, null, kdb$attr_num, kdb$num_values from mi ";
        query = (pK == 0) ? query + "where kdb$mi_rank is not null " : query + "where kdb$mi_rank = 1 ";
        query = query + "union all " +
               "select '" + className + "', 'KDB$CLASS', null, null, " + classIndex + ", count(*) from kdb_temp_ycounts";
//        System.out.println(query);
        runSQL(query, conn);
        conn.commit();

//        try {
//            runSQL("truncate table kdb_temp_ycounts", conn);
//            runSQL("truncate table kdb_temp_xycounts", conn);
//            runSQL("truncate table kdb_temp_xxycounts", conn);
//            runSQL("drop view " + viewName + " as " + pQuery, conn);
//        } catch (SQLException e) {}
        closeConnection(conn);
    }
    
    /**
     * KDB Pass 2 - create the model. Called directly when running in-database
     *
     * @param pQuery - a query returning the training data
     * @param pLabel - the class attribute
     * @param pK     - K - the number of parents
     * @param pModel - the model name - used in the model table name
     */
    public static void pass2Sql (String pQuery, String pLabel, int pK, String pModel) throws SQLException {
        String structure = "KDB$S_" + pModel.toUpperCase();
        String modelName = "KDB$M_" + pModel.toUpperCase();
        String query;
        PreparedStatement selectStmt;
        ResultSet rset;
        
        Connection conn = openConnection();

        try {
            runSQL("drop table " + modelName + " purge", conn);
        } catch (SQLException e) {}

        // Create the model table
        query = "create table " + modelName + " (" +
                "  kdb$attr_name     varchar2(30)," + 
                "  kdb$attr_value    varchar2(4000),";
        for ( int k = 1; k<= pK; k++) {
            query = query + "  kdb$parent" + k + "  varchar2(4000),";
        }
        query = query +
                "  kdb$class         varchar2(4000)," +
                "  kdb$count         number," +
                "  kdb$group_id      number," +
                "  kdb$num_values    number," +
                "  kdb$parents_count number) pctfree 0 nologging";
//        System.out.println(query);
        conn.prepareCall(query).execute();

        // Generate a list of attributes and their parents from the structure table
        query = "select * from " + structure + " pivot (max(kdb$attr_name2) for kdb$cmi_rank in (";
	String sepr = "";
	for ( int k = 1; k<= pK; k++) {
	    query = query + sepr + k + "  kdb$parent" + k;
	    sepr = ", ";
        }
	query = query + ")) order by kdb$mi_rank nulls first";
//        System.out.println(query);
        selectStmt = conn.prepareStatement(query);
        rset = selectStmt.executeQuery();

        // First attribute is the class
        rset.next();
        int numClasses = rset.getInt(4);

        // The next attribute has no parents
        rset.next();
        String attrName = rset.getString(1);
        int numAttrs = rset.getInt(4);
        query = "insert /*+ append */ into " + modelName + " " +
                "with x as ( " +
                    "select " + attrName + " kdb$attr_value, " + pLabel + " kdb$class, " +
                            "count(*) kdb$count, " +
                            "1 - grouping_id(" + attrName + ") kdb$group, " +
                            "nvl2(" + attrName + ", " + numAttrs + ", " + numClasses + ") kdb$numValues " +
                    "from (" + pQuery + ") " +
                    "group by " + pLabel + ", rollup(" + attrName + ") ) " +
                "select case when kdb$attr_value is not null then '" + attrName + "' end kdb$attr_name, kdb$attr_value";
        for ( int k = 1; k<= pK; k++) {
            query = query + ", null kdb$parent" + k ;
        }
        query = query + ", kdb$class, sum(kdb$count) kdb$count, kdb$group, kdb$numvalues, " +
                        "sum(sum(kdb$count)) over(partition by nvl2(kdb$attr_value, kdb$class, null), kdb$group) kdb$parents " +
                "from ( select * from x " +
                       "union all " +
                       "select kdb$attr_value, kdb$class, kdb$count, kdb$group, kdb$numvalues " +
                       "from ( select distinct kdb$attr_value, 0 kdb$count, kdb$numvalues, kdb$group " +
                              "from x), " +
                            "( select distinct kdb$class from x) ) " +
                "group by kdb$attr_value, kdb$class, kdb$numvalues, kdb$group";
//        System.out.println(query);
        runSQL("alter system flush buffer_cache", conn);
        runSQL(query, conn);
        conn.commit();

        // Process the remaining attributes
        while (rset.next()) {
            attrName = rset.getString(1);
            int miRank = rset.getInt(2);
            numAttrs = rset.getInt(4);
            int numParents = Math.min(miRank-1, pK);
            String thisParent;
            String parents1 = "";
            String parents2 = "";
            String parents3 = "";
            sepr = "";
            for (int p = 1; p <= numParents; p++) {
                thisParent = rset.getString(p+4);
                parents1 = parents1 + sepr + thisParent;
                parents2 = parents2 + thisParent + " kdb$parent" + p + ", ";
                parents3 = parents3 + ", kdb$parent" + p;
                sepr = ", ";
            }
            String parents4 = parents3;
            for (int p = numParents + 1; p <= pK; p++) {
                parents4 = parents4 + ", null kdb$parent" + p;
            }

            // Generate the first part of the insert - extract the attribute and parent columns 
            query = "insert /*+ append */ into " + modelName + " " +
                    "with x as ( " +
                        "select " + attrName + " kdb$attr_value, " + parents2 + pLabel + " kdb$class, count(*) kdb$count, " +
                                numParents + " - round(log(2, grouping_id(" + attrName + sepr + parents1 + ")+1)) kdb$group, " +
                                numAttrs + " kdb$numvalues " +
                        "from (" + pQuery + ") " +
                        "group by " + pLabel + ", " + attrName;
            if (pK == 0) query = query + ") ";
            // Generate the clauses to roll-up the counts by the parents
            else query = query + ", rollup(" + parents1 + ") ) ";
            // Generate the second part of the query - adds records for missing combinations
            query = query + "select * from ( " +
                    "select '" + attrName + "' kdb$attr_name, kdb$attr_value" + parents4 + ", " +
                           "kdb$class, sum(kdb$count) kdb$count, kdb$group, kdb$numvalues, " +
                           "sum(sum(kdb$count)) over(partition by kdb$class" + parents3 + ") kdb$parents " +
                    "from ( select * from x " +
                           "union all " +
                           "select kdb$attr_value" + parents3 + ", kdb$class, kdb$count, kdb$group, kdb$numvalues " +
                           "from ( select distinct kdb$attr_value" + parents3 + ", 0 kdb$count, kdb$group, kdb$numvalues " +
                                  "from x ), " +
                                "( select distinct kdb$class from x ) ) " +
                    "group by kdb$attr_value" + parents3 + ", kdb$class, kdb$numvalues, kdb$group " +
                    ")";
//            System.out.println(query);

            // Insert records for this attribute into the model table
            runSQL("alter system flush buffer_cache", conn);
            runSQL(query, conn);
            conn.commit();
        }
        rset.close();
        selectStmt.close();
        closeConnection(conn);
    }
}
