package KDB_inDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

/**
 * In-database KDB - Star-SQL method
 * Pass 1 is Star-SQL - Uses factorisation to process the dimension tables
 * Pass 2 is Grouping - Uses complex aggregations to calculate the conditional counts
 * 
 * @author lynnm
 */
public class KDBIDB7SQL {

//  Connection information for OOD testing    
    private static String m_DB = "localhost:1521:XE";
//    private static String m_DB = "rhino.its.monash.edu.au:1521:EDUDB";
    private static String m_User = "bayes";
    private static String m_Password = "bayes";

//  Default parameters
    private static int m_K = 2;
    private static String m_Query = "select * from nursery_input";
    private static String m_Table = "NURSERY_VIEW";
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

    /** For OOD testing - set up dimension parameters */
    private static ARRAY createDims() throws SQLException {
        Object[] attribs = new Object[4];
        Connection conn = openConnection();
        STRUCT[] dims = new STRUCT[4];
        StructDescriptor dimRec = new StructDescriptor("KDB$DIMREC",conn);
        attribs[0] = "EMPLOY_ID";
        attribs[1] = "EMPLOY_ID";
        attribs[2] = "NURSERY_EMPLOY";
        attribs[3] = "N";
        dims[0] = new STRUCT(dimRec, conn, attribs);
        attribs[0] = "STRUCTURE_ID";
        attribs[1] = "STRUCTURE_ID";
        attribs[2] = "NURSERY_STRUCTURE";
        attribs[3] = "N";
        dims[1] = new STRUCT(dimRec, conn, attribs);
        attribs[0] = "FINANCE_ID";
        attribs[1] = "FINANCE_ID";
        attribs[2] = "NURSERY_FINANCE";
        attribs[3] = "N";
        dims[2] = new STRUCT(dimRec, conn, attribs);
        attribs[0] = "SOC_HEALTH_ID";
        attribs[1] = "SOC_HEALTH_ID";
        attribs[2] = "NURSERY_SOC_HEALTH";
        attribs[3] = "N";
        dims[3] = new STRUCT(dimRec, conn, attribs);
        return new ARRAY(new ArrayDescriptor("KDB$DIMTAB",conn), conn, dims);
    } 

    /** Main is only used for OOD testing */
    public static void main(String[] args) {
        try {
            setOptions(args);
        } catch (Exception ex) {
            Logger.getLogger(KDBIDB3SQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
//          call pass 1 & 2 using no dimensions
//            kdbPass1(m_Table, m_Label, null, m_K, m_Model);
//            kdbPass2(m_Table, m_Label, null, m_K, m_Model);
//          Call pass 1 & 2 specifying fact and dimension tables  
            ARRAY dims = createDims();
            kdbPass1(m_Table, m_Label, dims, m_K, m_Model);
            kdbPass2(m_Table, m_Label, dims, m_K, m_Model);
//            printModel();
//            predict(100);
        } catch (SQLException e) {
            System.out.println("SQL error: " + e);
        }
    }

    /** 
     * KDB Pass 1 - create the structure. Called directly when running in-database 
     *
     * @param pFactTable - the fact table containing the class
     * @param pLabel     - the class attribute
     * @param pDimsArray - contains information about the dimension and dimension keys
     * @param pK         - K - the number of parents
     * @param pModel     - the model name - used in the structure table name
     */
    public static void kdbPass1(String pFactTable, String pLabel, ARRAY pDimsArray, int pK, String pModel) throws SQLException {
        String structure = "KDB$S_" + pModel.toUpperCase();
        String className = pLabel.toUpperCase();
        HashMap<String, Integer> attrIndexes = new HashMap<String, Integer>();
        boolean[] includeAttr;
        STRUCT[] pDims = null;
        String query;
        PreparedStatement selectStmt;
        ResultSet rset;
        Connection conn = openConnection();
        
        if (pDimsArray != null) {
            Datum[] pDatum = pDimsArray.getOracleArray();
            pDims = new STRUCT[pDatum.length];
            for (int d=0; d<pDatum.length; d++) pDims[d] = (STRUCT) pDatum[d];
        }

        runSQL("truncate table kdb_temp_ycounts", conn);
        runSQL("truncate table kdb_temp_xycounts", conn);
        runSQL("truncate table kdb_temp_xxycounts", conn);
        try {
            runSQL("drop table " + structure + " purge", conn);
        } catch (SQLException e) {}

        // Process the fact table
        selectStmt = conn.prepareStatement("select column_name " +
                "from user_tab_columns " +
                "where table_name = '" + pFactTable.toUpperCase() + "'" +
                "order by column_id");
        rset = selectStmt.executeQuery();
        rset.next();
        String attrName = rset.getString(1);
        int classIndex = 0;
        int attrNum = 0;
        if (attrName.equals(className)) { 
            classIndex = attrNum;
            attrIndexes.put(attrName, attrNum);
            attrNum++;
            rset.next();
            attrName = rset.getString(1);
        }
        query = "insert /*+ append */ into kdb_temp_xycounts " +
                "select '" + attrName + "' kdb$attr_name, " + attrName + " kdb$attr_value, " +
                        className + " kdb$class, count(*) kdb$count, " + attrNum + " kdb$attr_num, " +
                        "count(distinct nvl(to_char(" + attrName + "),'~')) over () kdb$num_values " +
                "from " + pFactTable + " " +
                "group by " + attrName + ", " + className;
//        System.out.println(query);
        runSQL("alter system flush buffer_cache", conn);
        runSQL(query, conn);
        conn.commit();
        runSQL("insert /*+ append */ into kdb_temp_ycounts "
                + "select kdb$class, sum(kdb$count) kdb$count "
                + "from kdb_temp_xycounts "
                + "group by kdb$class", conn);
        conn.commit();
        attrIndexes.put(attrName, attrNum);
        String attrList = attrName;
        String attrList2 = "to_char(" + className + ") kdb$class" + ", to_char(" + attrName + ") " + attrName;
        while (rset.next()) {
            attrName = rset.getString(1);
            attrNum++;
            attrIndexes.put(attrName, attrNum);
            if (attrName.equals(className)) classIndex = attrNum;
            else {
                attrList = attrList + ", " + attrName;
                attrList2 = attrList2 + ", to_char(" + attrName + ") " + attrName;
                if (pK == 0) {
                    query = "insert /*+ append */ into kdb_temp_xycounts " +
                        "select '" + attrName + "' kdb$attr_name, " + attrName + " kdb$attr_value, " +
                                className + " kdb$class, count(*) kdb$count, " + attrNum + " kdb$attr_num, " +
                                "count(distinct nvl(to_char(" + attrName + "),'~')) over () kdb$num_values " +
                        "from " + pFactTable + " " +
                        "group by " + attrName + ", " + className;
                } else {
                    query = "insert /*+ append */ first " +
                            "when kdb$num_attrs = 1 then into kdb_temp_xycounts " + 
                                "values(kdb$attr_name1, kdb$attr_value1, kdb$class, kdb$count, kdb$attr_num, kdb$num_values) " +
                            "else into kdb_temp_xxycounts " + 
                                "values(kdb$attr_name1, kdb$attr_value1, kdb$attr_name2, kdb$attr_value2, kdb$class, kdb$count) " +
                            "select case when kdb$attr_name2 = '" + attrName + "' then 1 else 2 end kdb$num_attrs, " +
                                   "'" + attrName + "' kdb$attr_name1, kdb$attr_value1, " +
                                   "kdb$attr_name2, kdb$attr_value2, " +
                                   "kdb$class, count(*) kdb$count, " + attrNum + " kdb$attr_num, " +
                                   "count(distinct nvl(kdb$attr_value1, '~')) over() kdb$num_values " +
                            "from ( select kdb$attr_value1, kdb$class, " + 
                                          "kdb$attr_name2, kdb$attr_value2 " +
                                   "from ( select to_char(" + attrName + ") kdb$attr_value1, " + attrList2 + " from " + pFactTable + ") " +
                                   "unpivot (kdb$attr_value2 for kdb$attr_name2 in (" + attrList + ")) ) " +
                            "group by kdb$attr_value1, kdb$class, kdb$attr_name2, kdb$attr_value2";
                }
                System.out.println(query);
                runSQL("alter system flush buffer_cache", conn);
                runSQL(query, conn);
                conn.commit();
            }
        }
        int numFactAttrs = attrNum + 1;

        // Process dimensions, if provided
        int numDims = 0;
        Dimension[] dims = null;
        if (pDims != null) {
            numDims = pDims.length;
            dims = new Dimension[numDims];

            for (int d=0; d < numDims; d++) {
                dims[d] = new Dimension(pDims[d]);
                dims[d].setFirstAttrNum(attrNum+1);
                selectStmt = conn.prepareStatement("select column_name " +
                    "from user_tab_columns " +
                    "where table_name = '" + dims[d].getTableName() + "'" +
                    "order by column_id");
                rset = selectStmt.executeQuery();
                while (rset.next()) {
                    attrName = rset.getString(1);
                    attrNum++;
                    if (attrName.equals(dims[d].getPkeyName())) {
                        dims[d].setPkeyAttrNum(attrNum);
                        dims[d].setFkeyAttrNum(attrIndexes.get(dims[d].getFkeyName()));
                    }
                    else {
                        query = "insert /*+ append */ into kdb_temp_xycounts " +
                                "select '" + attrName + "' kdb$attr_name, d." + attrName + " kdb$attr_value, " +
                                       "c.kdb$class, sum(c.kdb$count) kdb$count, " + attrNum + " kdb$attr_num, " + 
                                       "count(distinct nvl(to_char(d." + attrName + "), '~')) over () kdb$num_values " +
                                "from kdb_temp_xycounts c, " + dims[d].getTableName() + " d " +
                                "where c.kdb$attr_name = '" + dims[d].getFkeyName() + "' " +
                                  "and c.kdb$attr_value = d." + dims[d].getPkeyName() + " " +
                                "group by d." + attrName + ", c.kdb$class";
//                        System.out.println(query);
                        runSQL(query, conn);
                        conn.commit();
                        if (pK > 0) {
                            query = "insert /*+ append */ into kdb_temp_xxycounts " +
                                    "select '" + attrName + "' kdb$attr_name1, d." + attrName + " kdb$attr_value1, " +
                                           "c.kdb$attr_name2, c.kdb$attr_value2, c.kdb$class, sum(c.kdb$count) kdb$count " +
                                    "from ( select kdb$attr_value1, kdb$attr_name2, kdb$attr_value2, kdb$class, kdb$count " +
                                           "from kdb_temp_xxycounts " +
                                           "where kdb$attr_name1 = '" + dims[d].getFkeyName() + "' " +
                                           "union all " +
                                           "select kdb$attr_value2, kdb$attr_name1, kdb$attr_value1, kdb$class, kdb$count " +
                                           "from kdb_temp_xxycounts " +
                                           "where kdb$attr_name2 = '" + dims[d].getFkeyName() + "' " +
                                         ") c, " +
                                          dims[d].getTableName() + " d " +
                                    "where c.kdb$attr_value1 = d." + dims[d].getPkeyName() + " " +
                                    "group by d." + attrName + ", c.kdb$attr_name2, c.kdb$attr_value2, c.kdb$class";
                            query = query + " union all " +
                                    "select '" + attrName + "' kdb$attr_name1, d." + attrName + " kdb$attr_value2, " +
                                           "c.kdb$attr_name kdb$attr_name2, c.kdb$attr_value kdb$attr_value2, " +
                                           "c.kdb$class, kdb$count " +
                                    "from kdb_temp_xycounts c, " + dims[d].getTableName() + " d " +
                                    "where c.kdb$attr_name = '" + dims[d].getFkeyName() + "' " +
                                      "and c.kdb$attr_value = d." + dims[d].getPkeyName() + " ";                                
//                            System.out.println(query);
                            runSQL(query, conn);
                            conn.commit();
                        }
                    }
                }
                dims[d].setNumAttrs(attrNum + 1 - dims[d].getFirstAttrNum());
                if (!dims[d].getKeyIsAttr()) {
                    query = "delete kdb_temp_xycounts where kdb$attr_name = '" + dims[d].getFkeyName() + "'";
//                    System.out.println(query);
                    runSQL(query, conn);
                    conn.commit();
                    query = "delete kdb_temp_xxycounts where kdb$attr_name1 = '" + dims[d].getFkeyName() +
                            "' or kdb$attr_name2 = '" + dims[d].getFkeyName() + "'";
//                    System.out.println(query);
                    runSQL(query, conn);
                    conn.commit();
                }
            }
        }
        
        // Flag attributes included/excluded from structure
        includeAttr = new boolean[attrNum+1];
        for (int i=0; i <= attrNum; i++) {
            includeAttr[i] = i != classIndex;
        }
        for (int d=0; d < numDims; d++) {
            includeAttr[dims[d].getPkeyAttrNum()] = false;
            includeAttr[dims[d].getFkeyAttrNum()] = dims[d].getKeyIsAttr();
        }

        // Construct the query to create the structure table from temporary tables
        query = "create table " + structure + " pctfree 0 as " +
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
                             "union all " +
                             "select kdb$attr_name2, kdb$attr_value2, kdb$attr_name1, " +
                                   " kdb$attr_value1, kdb$class, kdb$count " +
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
           "select cast (case when kdb$attr_num < " + numFactAttrs + " then '" + pFactTable + "' ";
        for ( int d = 0; d < numDims; d++) {
            query = query + "when kdb$attr_num <= " + dims[d].getLastAttrNum() + " then '" + dims[d].getTableName() + "' ";
        }
        query = query + "end as varchar2(30)) kdb$table_name, kdb$attr_name, kdb$attr_name2, kdb$mi_rank, kdb$cmi_rank, kdb$attr_num, kdb$num_values from (" +
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
               "select '" + className + "', 'KDB$CLASS', null, null, " + classIndex + ", count(*) from kdb_temp_ycounts)";
//        System.out.println(query);
        runSQL(query, conn);
        
        // Insert records for the dimension keys (for compatibility with Star-Java)
        for (int d = 0; d < numDims; d++) {
            runSQL("insert into " + structure + " (kdb$table_name, kdb$attr_name, kdb$attr_num) " +
                   "values('" + dims[d].getTableName() + "', '" + dims[d].getPkeyName() + "', " + dims[d].getPkeyAttrNum() + ")", conn);
            if (!dims[d].getKeyIsAttr())
                runSQL("insert into " + structure + " (kdb$table_name, kdb$attr_name, kdb$attr_num) " +
                       "values('" + pFactTable + "', '" + dims[d].getFkeyName() + "', " + dims[d].getFkeyAttrNum() + ")", conn);
        }
        conn.commit();
//        try {
//            runSQL("truncate table kdb_temp_ycounts", conn);
//            runSQL("truncate table kdb_temp_xycounts", conn);
//            runSQL("truncate table kdb_temp_xxycounts", conn);
//        } catch (SQLException e) {}
        closeConnection(conn);
    }

    /**
     * KDB Pass 2 - create the model. Called directly when running in-database
     *
     * @param pFactTable - the fact table containing the class
     * @param pLabel     - the class attribute
     * @param pDimsArray - contains information about the dimension and dimension keys
     * @param pK         - K - the number of parents
     * @param pModel     - the model name - used in the model table name
     */
    public static void kdbPass2(String pFactTable, String pLabel, ARRAY pDimsArray, int pK, String pModel) throws SQLException {
        String structure = "KDB$S_" + pModel.toUpperCase();
        String modelName = "KDB$M_" + pModel.toUpperCase();
        String query;
        String pQuery;
        STRUCT[] pDims = null;
        PreparedStatement selectStmt;
        ResultSet rset;

        String grouping = "";
        String[] caseStmts = new String[pK+2];
        String attrList;
        String attrName;
        String parent;
        String parentList = "";
        
        Connection conn = openConnection();

        if (pDimsArray != null) {
            Datum[] pDatum = pDimsArray.getOracleArray();
            pDims = new STRUCT[pDatum.length];
            for (int d=0; d<pDatum.length; d++) pDims[d] = (STRUCT) pDatum[d];
        }

        try {
            runSQL("drop table " + modelName + " purge", conn);
        } catch (SQLException e) {}

        // Construct a query to join fact and dimension tables
        query = "select listagg('to_char(' || kdb$attr_name || ') ' || kdb$attr_name, ', ') within group (order by kdb$mi_rank), " +
                       "listagg(kdb$table_name || '.' || kdb$attr_name, ', ') within group (order by kdb$attr_num) " +
                "from (select distinct kdb$table_name, kdb$attr_name, kdb$attr_num, kdb$mi_rank from " + structure + " where kdb$mi_rank is not null)";
//        System.out.println(query);
        selectStmt = conn.prepareStatement(query);
        rset = selectStmt.executeQuery();
        rset.next();
        attrList = rset.getString(1);
        String selectClause = "select " + rset.getString(2) + ", " + pFactTable + "." + pLabel;
        rset.close();
        selectStmt.close();

        String fromClause = " from " + pFactTable;
        String whereClause = " where 1=1";
        if (pDims != null) {
            int numDims = pDims.length;
            Dimension[] dims = new Dimension[numDims];

            for (int d=0; d < numDims; d++) {
                dims[d] = new Dimension(pDims[d]);
                fromClause = fromClause + ", " + dims[d].getTableName();
                whereClause = whereClause + " and " + pFactTable + "." + dims[d].getPkeyName() +
                        " = " + dims[d].getTableName() + "." + dims[d].getFkeyName();
            }
        }
        pQuery = selectClause + fromClause + whereClause;
//        System.out.println(pQuery);

        // Build the case statements used in model construction query
        caseStmts[0] = "case kdb$attr_name";
        caseStmts[pK+1] = "case";
        for (int p = 1; p <= pK; p++) {
            caseStmts[p] = ", case kdb$attr_name";
        }
        query = "select * from ( select kdb$attr_name, kdb$mi_rank, kdb$cmi_rank, kdb$attr_name2" +
                               " from " + structure +
                               " where kdb$mi_rank is not null ) " +
                "pivot (max(kdb$attr_name2) for kdb$cmi_rank in (";
	String sepr = "";
	for ( int k = 1; k <= pK; k++) {
	    query = query + sepr + k + " kdb$parent" + k;
            parentList = parentList + sepr + "kdb$parent" + k;
	    sepr = ", ";
        }
	query = query + ")) order by kdb$mi_rank desc";
//        System.out.println(query);
        selectStmt = conn.prepareStatement(query);
        rset = selectStmt.executeQuery();
        while (rset.next()) {
            attrName = rset.getString(1);
            caseStmts[0] = caseStmts[0] + " when '" + attrName + "' then " + attrName;
            caseStmts[pK+1] = caseStmts[pK+1] + " when grouping(" + attrName + ") = 0 then '" + attrName + "'";
            grouping = grouping + "(" + attrName;
            for (int p = 1; p <= pK; p++) {
                parent = rset.getString(p+2);
                if (parent != null) {
                    caseStmts[p] = caseStmts[p] + " when '" + attrName + "' then " + parent;
                    grouping = grouping + ", " + parent;
                }
            }
            grouping = grouping + "), ";
        }
        rset.close();
        selectStmt.close();
        caseStmts[0] = caseStmts[0] + " end kdb$attr_value";
        caseStmts[pK+1] = caseStmts[pK+1] + " end kdb$attr_name ";
        grouping = grouping + "null";
        for (int p = 1; p <= pK; p++) caseStmts[p] = caseStmts[p] + " else '~' end kdb$parent" + p;

        // Construct SQL statement to create the model table
        if (pK > 0) {
            query = "create table " + modelName + " pctfree 0 nologging as " +
                    "with x as ( " +
                        "select kdb$attr_name, kdb$attr_value, " + parentList + ", kdb$class, sum(kdb$count) kdb$count, " + 
                                pK + " - round(log(2,grouping_id(kdb$attr_name, " + parentList + ")+1)) kdb$group_id " +
                        "from ( select kdb$attr_name, kdb$class, kdb$count, " + caseStmts[0];
            for (int p = 1; p <= pK; p++) query = query + caseStmts[p];
            query = query + " from ( select " + attrList + ", " + pLabel + " kdb$class, count(*) kdb$count, " + caseStmts[pK+1] + 
                                    "from (" + pQuery + ") " + 
                                    "group by " + pLabel + ", grouping sets (" + grouping + ") )) " +
                        "group by kdb$class, kdb$attr_name, kdb$attr_value, rollup(" + parentList + ")";
            parentList = parentList + ", ";
        } else {
            query = "create table " + modelName + " pctfree 0 as " +
                    "with x as ( " +
                        "select kdb$attr_name, kdb$attr_value, kdb$class, sum(kdb$count) kdb$count, " + 
                                pK + " - round(log(2,grouping_id(kdb$attr_name)+1)) kdb$group_id " +
                        "from ( select kdb$attr_name, kdb$class, kdb$count, " + caseStmts[0] +
                              " from ( select " + attrList + ", " + pLabel + " kdb$class, count(*) kdb$count, " + caseStmts[1] + 
                                    "from (" + pQuery + ") " + 
                                    "group by " + pLabel + ", grouping sets (" + grouping + ") )) " +
                        "group by kdb$class, kdb$attr_name, kdb$attr_value"; 

        }
        query = query + " having group_id() = 0 ) " +
                "select kdb.* " + 
                "from ( select kdb$attr_name, kdb$attr_value, " + parentList + "kdb$class, max(kdb$count) kdb$count, kdb$group_id, " +
                       "count(distinct kdb$attr_value) over (partition by kdb$attr_name) kdb$num_values, " +
                       "sum(max(kdb$count)) over (partition by kdb$attr_name, " + parentList + "nvl2(kdb$attr_name, kdb$class, null)) kdb$parents_count " +
                       "from ( select kdb$attr_name, kdb$attr_value, " + parentList + "kdb$class, kdb$count, kdb$group_id from x " +
                              "union all " +
                              "select kdb$attr_name, kdb$attr_value, " + parentList + "kdb$class, kdb$count, kdb$group_id " +
                              "from (select distinct kdb$attr_name, kdb$attr_value, " + parentList + " 0 kdb$count, kdb$group_id from x), " +
                                   "(select distinct kdb$class from x) ) " +
                       "group by kdb$attr_name, kdb$attr_value, " + parentList + "kdb$class, kdb$group_id " +
                     ") kdb " +
                "where 1=1";
        for (int p = 1; p <= pK; p++) query = query + " and nvl(kdb$parent" + p + ",'#') != '~'";
        
//        System.out.println(query);
        runSQL(query, conn);
        closeConnection(conn);
    }
}
