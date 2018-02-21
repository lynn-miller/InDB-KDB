package KDB_inDB;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

/**
 * In-database KDB - Star-Java method
 * Pass 1 is Star-Java - Uses factorisation to process the dimension tables
 * Pass 2 not used for results - reuses inDB-Java, adapted to construct a query
 *     from the fact/dimension tables
 *
 * @author lynnm
 */
public class KDBIDB6SQL { 
//    private static String m_DB = "localhost:1521:XE";
    private static String m_DB = "rhino.its.monash.edu.au:1521:EDUDB";
    private static String m_User = "bayes";
    private static String m_Password = "";
    private static int m_K = 2;
    private static String m_Query = "select * from nursery_input";
    private static String m_Label = "OUTCOME";
    private static String m_Table = "NURSERY_INPUT";
    private static boolean m_Trace = false;
    private static String m_Model = "unnamed";
    
    static BayesParametersTree dParameters_;

    /** Get CPU time in nanoseconds. */
    private static long getCpuTime( ) {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
        return bean.isCurrentThreadCpuTimeSupported( ) ?
            bean.getCurrentThreadCpuTime( ) : 0L;
    }

    /**
     * Retrieve the connection when using in-database JVM
     * Else connect to the database using connect string, user name and password
     */
    private static Connection openConnection() {
        Connection conn = null;
        try {
            if (System.getProperty ("oracle.jserver.version") == null) {
                conn = DriverManager.getConnection("jdbc:oracle:thin:@" + m_DB, m_User, m_Password);
                conn.setAutoCommit (false);
            } else {
                conn = DriverManager.getConnection("jdbc:default:connection:");
            }
            System.out.println("Connected to " + m_DB + " database");
        } catch(SQLException e) {
            System.out.println("Connection error. Cannot connect to " + m_DB + " database");
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
    
    /** Save performance statistics and close the OOD connection */
    private static void saveStats(String step, String model) {
        System.gc();
        long cpuTimeNano = getCpuTime();
        long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        Connection conn = openConnection();
        try {
            runSQL("alter system flush buffer_cache", conn);
            CallableStatement callStmt = conn.prepareCall("begin proc_save_java_stats('Java-dims','" + model + "','" + step + "'," +
                    cpuTimeNano + "," + usedMem + "); end;");
            callStmt.execute();
            callStmt.close();
            if (System.getProperty ("oracle.jserver.version") == null) {
                conn.close();
                System.out.println("Database connection closed");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving data! " + e.getMessage());
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
        attribs[0] = "WSTORE";
        attribs[1] = "WSTORE";
        attribs[2] = "WALMART_STORES";
        attribs[3] = "Y";
        dims[0] = new STRUCT(dimRec, conn, attribs);
        attribs[0] = "WEEK";
        attribs[1] = "WEEK";
        attribs[2] = "WALMART_WEEKS";
        attribs[3] = "Y";
        dims[1] = new STRUCT(dimRec, conn, attribs);
        attribs[0] = "TEMPERATURE_IND";
        attribs[1] = "TEMPERATURE_IND";
        attribs[2] = "WALMART_TEMPERATURES";
        attribs[3] = "N";
        dims[2] = new STRUCT(dimRec, conn, attribs);
        attribs[0] = "ECONOMIC_IND";
        attribs[1] = "ECONOMIC_IND";
        attribs[2] = "WALMART_ECONOMICS";
        attribs[3] = "N";
        dims[3] = new STRUCT(dimRec, conn, attribs);
        return new ARRAY(new ArrayDescriptor("KDB$DIMTAB",conn), conn, dims);
    } 

    /** Main is only used for OOD testing */
    public static void main(String[] args) {
        try {
            setOptions(args);
        } catch (Exception ex) {
            Logger.getLogger(KDBIDB6SQL.class.getName()).log(Level.SEVERE, null, ex);
        }
//        saveStats("Start");
        try {
//          call pass 1 & 2 using no dimensions
            kdbPass1(m_Table, m_Label, null, m_K, m_Model);
            kdbPass2(m_Table, m_Label, null, m_K, m_Model);
//          Call pass 1 & 2 specifying fact and dimension tables  
            ARRAY dimArray = createDims();
//            kdbPass1("WALMART_VIEW", "WEEKLY_SALES", dimArray, m_K, m_Model);
//            kdbPass2("WALMART_VIEW", "WEEKLY_SALES", dimArray, m_K, m_Model);
            printModel();
//            predict(m_Query, 100);
        } catch (SQLException ex) {
            Logger.getLogger(KDBIDB6SQL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Calculate MIs from counts
     * Copied from Fupla CorrelationMeasures.java class getMutualInformation method
     * with minor alterations
     */
    private static void getMI(xyDist xyDist_, boolean[] includeAttr, double[] mi) {
        double m;
        int avyCount;
        int nc = xyDist_.getNoClasses();
        int n = xyDist_.getNoAtts();
        double N = xyDist_.getNoData();

        int[] paramsPerAtt = new int[n];
        for (int u = 0; u < n; u++) {
            paramsPerAtt[u] = xyDist_.getNoValues(u);		
        }

        for (int u = 0; u < n; u++) {
            if (includeAttr[u]) {
                m = 0;
                for (int uval = 0; uval < paramsPerAtt[u]; uval++) {
                    for (int y = 0; y < nc; y++) {
                        avyCount = xyDist_.getCount(u, uval, y);
                        if (avyCount > 0) {
                            m += (avyCount / N) * Math.log( avyCount / ( xyDist_.getCount(u, uval)/N * xyDist_.getClassCount(y) ) ) / Math.log(2);
                        }
                    }
                }
                mi[u] = m;
            } else mi[u] = -1;
        }
    }

    /** 
     * Calculate CMIs from counts
     * Copied from Fupla CorrelationMeasures.java class getCondMutualInf method
     * with minor alterations
     */
    private static void getCMI(xxyDist xxyDist_, boolean[] includeAttr, double[][] cmi) {
        double mi, a, b, d, e, avvyCount, mitemp;

        int nc = xxyDist_.getNoClasses();
        int n = xxyDist_.getNoAtts();
        double N = xxyDist_.getNoData();

        int[] paramsPerAtt = new int[n];
        for (int u = 0; u < n; u++) {
            paramsPerAtt[u] = xxyDist_.getNoValues(u);		
        }

        // Calculate conditional mutual information		
        for (int u1 = 1; u1 < n; u1++) {
            for (int u2 = 0; u2 < u1; u2++) {
                if (includeAttr[u1] && includeAttr[u2] ) {
                    mi = 0;
                    for (int u1val = 0; u1val < paramsPerAtt[u1]; u1val++) {					
                        for (int u2val = 0; u2val < paramsPerAtt[u2]; u2val++) {
                            for (int c = 0; c < nc; c++) {		
                                avvyCount = xxyDist_.getCount(u1, u1val, u2, u2val, c);
                                if (avvyCount > 0) {															
                                    a = avvyCount;
                                    b = xxyDist_.xyDist_.getClassCount(c);
                                    d = xxyDist_.xyDist_.getCount(u1, u1val, c);
                                    e = xxyDist_.xyDist_.getCount(u2, u2val, c);
                                    mitemp = (a/N) * Math.log((a*b) / (d*e)) / Math.log(2);
                                    mi += mitemp;
                                }
                            }
                        }
                    }
                    cmi[u1][u2] = mi;
                    cmi[u2][u1] = mi;
                }
            }
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
    public static void kdbPass1(String pFactTable, String pLabel, ARRAY pDimsArray, int pK, String pModel) {

        String factQuery;
        String[] attributes;
        String[] attTables;
        ArrayList[] attValues;
        STRUCT[] pDims = null;
        boolean[] includeAttr;
        int labelIndex = -1;
        int nc;
        int N;
        int numCols;
        String nullChar = "~";

        int[][] m_Parents;
        int[] m_Order;

        int[] attCounts;
	Instance row;
        xxyDist xxyCounts = null;

        PreparedStatement selectStmt;
        PreparedStatement insertStmt;
        Connection conn;
        ResultSet rset;
        
        saveStats("Start", pModel);
        conn = openConnection();
        try {
            if (pDimsArray != null) {
                Datum[] pDatum = pDimsArray.getOracleArray();
                pDims = new STRUCT[pDatum.length];
                for (int d=0; d<pDatum.length; d++) pDims[d] = (STRUCT) pDatum[d];
            }

            // Extract the fact attribute values
            factQuery = "select * from " + pFactTable;
            AttrValues factValues = new AttrValues(factQuery, pLabel, conn, nullChar);

            numCols = factValues.getNumCols();
            N = factValues.getNumObs();
            nc = factValues.getNumClasses();
            labelIndex = factValues.getClassIdx();
            
            // Extract the dimension attribute values
            int numDims = (pDims == null) ? 0 : pDims.length;
            Dimension[] dims = new Dimension[numDims];
            AttrValues[] dimValues = new AttrValues[numDims];
            String[] dimQueries = new String[numDims];
            
            for (int d=0; d < numDims; d++) {
                dims[d] = new Dimension((STRUCT) (pDims[d]));
                dimQueries[d] = "select * from " + dims[d].getTableName();
                dimValues[d] = new AttrValues(dimQueries[d], dims[d].getFkeyName(), conn, nullChar);
                int dimCols = dimValues[d].getNumCols();
                dims[d].setNumAttrs(dimCols);
                dims[d].setFirstAttrNum(numCols);
                numCols += dimCols;
            }
            
            attributes = new String[numCols];
            attTables = new String[numCols];
            attValues = new ArrayList[numCols];
            includeAttr = new boolean[numCols];

            // Retrieve fact attributes and values
            for (int f=0; f < factValues.getNumCols(); f++) {
                attributes[f] = factValues.getAttributes()[f];
                attTables[f] = pFactTable;
                attValues[f] = factValues.getAttValues()[f];
                includeAttr[f] = f != labelIndex;
            }

            // Retrieve dimension attributes and values
            for (int d=0; d < numDims; d++) {
                int firstCol = dims[d].getFirstAttrNum();
                int fkeyIndex = factValues.getAttIndex(dims[d].getFkeyName());
                for (int i=0; i < dims[d].getNumAttrs(); i++) {
                    attributes[firstCol+i] = dimValues[d].getAttributes()[i];
                    attTables[firstCol+i] = dims[d].getTableName();
                    if (i == dimValues[d].getClassIdx()) {
                        attValues[firstCol+i] = attValues[fkeyIndex];
                        includeAttr[firstCol+i] = false;
                        includeAttr[fkeyIndex] = dims[d].getKeyIsAttr();
                        dimValues[d].setAttValues(i, attValues[fkeyIndex]);
                    } else {
                        attValues[firstCol+i] = dimValues[d].getAttValues()[i];
                        includeAttr[firstCol+i] = true;
                    }
                }
            }

            attCounts = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                attCounts[i] = attValues[i].size();
            }
            xyDist xyCounts;
            if (pK > 0) {
                xxyCounts = new xxyDist(N, numCols, nc, labelIndex, attCounts);
                xyCounts = xxyCounts.xyDist_;
            } else xyCounts = new xyDist(N, numCols, nc, labelIndex, attCounts);

            // Process the fact table
            selectStmt = conn.prepareStatement(factQuery);
            selectStmt.setFetchSize(1000);
            rset = selectStmt.executeQuery();
	    N = 0;
            while (rset.next()) {
                row = new Instance(rset, factValues.getAttValues(), labelIndex, true, nullChar);
                if (pK > 0) xxyCounts.updateFact(row, factValues.getNumCols());
                else xyCounts.updateFact(row, factValues.getNumCols());
                N++;
            }
            rset.close();
            selectStmt.close();

            // Process dimensions, if provided
            for (int d = 0; d < numDims; d++) {
                selectStmt = conn.prepareStatement(dimQueries[d]);
                selectStmt.setFetchSize(1000);
                rset = selectStmt.executeQuery();
                while (rset.next()) {
                    row = new Instance(rset, dimValues[d].getAttValues(), dimValues[d].getClassIdx(), false, nullChar);
                    if (row.classValue() != -1)
                        if (pK > 0) xxyCounts.updateDim(row, dims[d].getFirstAttrNum(), dims[d].getFirstAttrNum()+dims[d].getNumAttrs(), factValues.getAttIndex(dims[d].getFkeyName()));
                        else xyCounts.updateDim(row, dims[d].getFirstAttrNum(), dims[d].getFirstAttrNum()+dims[d].getNumAttrs(), factValues.getAttIndex(dims[d].getFkeyName()));
                 }
            }
            
            double[] mi = new double[attributes.length];
            getMI(xyCounts, includeAttr, mi);
            double[][] cmi = new double[numCols][numCols];
            if (pK > 0) getCMI(xxyCounts, includeAttr, cmi);
            
            // Sort attributes on MI with the class
            m_Order = SUtils.sort(mi);
            m_Parents = new int[attributes.length][];

            // Calculate parents based on MI and CMI
            for (int u = 0; u < attributes.length - 1; u++) {
                int nK = Math.min(u, pK);
                if (nK > 0) {
                    m_Parents[u] = new int[nK];
                    double[] cmi_values = new double[u];
                    for (int j = 0; j < u; j++) {
                        cmi_values[j] = cmi[m_Order[u]][m_Order[j]];
                    }
                    int[] cmiOrder = SUtils.sort(cmi_values);
                    for (int j = 0; j < nK; j++) {
                        m_Parents[u][j] = m_Order[cmiOrder[j]];
                    }
                }
            }

            // Save the structure
            try {
                runSQL("drop table KDB$S_" + pModel, conn);
            } catch (SQLException e) {}
            runSQL("create table KDB$S_" + pModel
                    + " (kdb$attr_num   number,"
                    + "  kdb$table_name varchar2(30),"
                    + "  kdb$attr_name  varchar2(30),"
                    + "  kdb$attr_name2 varchar2(30),"
                    + "  kdb$mi_rank    number,"
                    + "  kdb$cmi_rank   number,"
                    + "  kdb$num_values number)", conn);
            for (int i = 0; i < attributes.length; i++) {
                if (includeAttr[m_Order[i]]) {
                    if (m_Parents[i] != null) {
                        for (int j = 0; j < m_Parents[i].length; j++) {
                            insertStmt = conn.prepareStatement("insert into KDB$S_"
                                    + pModel + " values(?, ?, ?, ?, ?, ?, ?)" );
                            insertStmt.setInt(1, m_Order[i]);
                            insertStmt.setString(2, attTables[m_Order[i]]);
                            insertStmt.setString(3, attributes[m_Order[i]]);
                            insertStmt.setString(4, attributes[m_Parents[i][j]]);
                            insertStmt.setInt(5, i+1);
                            insertStmt.setInt(6, j+1);
                            insertStmt.setInt(7, attCounts[m_Order[i]]);
                            insertStmt.executeUpdate();
                            insertStmt.close();
                        }
                    } else {
                        insertStmt = conn.prepareStatement("insert into KDB$S_"
                                + pModel + " values(?, ?, ?, null, ?, null, ?)" );
                        insertStmt.setInt(1, m_Order[i]);
                        insertStmt.setString(2, attTables[m_Order[i]]);
                        insertStmt.setString(3, attributes[m_Order[i]]);
                        insertStmt.setInt(4, i+1);
                        insertStmt.setInt(5, attCounts[m_Order[i]]);
                        insertStmt.executeUpdate();
                        insertStmt.close();
                    }
                } else {
                    insertStmt = conn.prepareStatement("insert into KDB$S_"
                            + pModel + " values(?, ?, ?, ?, null, null, ?)" );
                    insertStmt.setInt(1, m_Order[i]);
                    insertStmt.setString(2, attTables[m_Order[i]]);
                    insertStmt.setString(3, attributes[m_Order[i]]);
                    insertStmt.setInt(5, attCounts[m_Order[i]]);
                    if (m_Order[i] == labelIndex) insertStmt.setString(4, "KDB$CLASS");
                    else insertStmt.setString(4, null);
                    insertStmt.executeUpdate();
                    insertStmt.close();
                }
            }
            conn.commit();

            if (System.getProperty ("oracle.jserver.version") == null) {
                conn.close();
                System.out.println("Database connection closed");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving data! " + e.getMessage());
        }
        saveStats("Pass1", pModel);
        xxyCounts = null;
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
    public static void kdbPass2(String pFactTable, String pLabel, ARRAY pDimsArray, int pK, String pModel) {
        int numAttrs;
        Instance row;
        ArrayList[] attValues;
        STRUCT[] pDims = null;
        String nullChar = "~";

        PreparedStatement selectStmt;
        Connection conn;
        ResultSet rset;
        
        conn = openConnection();
        try {
            if (pDimsArray != null) {
                Datum[] pDatum = pDimsArray.getOracleArray();
                pDims = new STRUCT[pDatum.length];
                for (int d=0; d<pDatum.length; d++) pDims[d] = (STRUCT) pDatum[d];
            }

            // Initialise the Bayes Parameter tree
            dParameters_ = new BayesParametersTree(conn, pModel);
            numAttrs = dParameters_.getNumCols();
            attValues = new ArrayList[numAttrs];
            for (int i = 0; i < numAttrs; i++) {
                attValues[i] = new ArrayList();
            }

            // Construct a query to join fact and dimension tables
            String selectClause = "select f.*";
            String fromClause = " from " + pFactTable + " f";
            String whereClause = " where 1=1";
            
            int numDims = (pDims == null) ? 0 : pDims.length;
            Dimension[] dims = new Dimension[numDims];
            
            for (int d=0; d < numDims; d++) {
                dims[d] = new Dimension(pDims[d]);
                String dAlias = "d" + d;
                selectClause = selectClause + ", " + dAlias + ".*";
                fromClause = fromClause + ", " + dims[d].getTableName() + " " + dAlias;
                whereClause = whereClause + " and f." + dims[d].getPkeyName() +
                        " = " + dAlias + "." + dims[d].getFkeyName();
            }

            // Build the model
            selectStmt = conn.prepareStatement(selectClause + fromClause + whereClause);
            selectStmt.setFetchSize(1000);
            rset = selectStmt.executeQuery();
            while (rset.next()) {
                row = new Instance(rset, attValues, dParameters_.getClassIndex(), true, nullChar);
                dParameters_.update(row);
            }
            rset.close();
            selectStmt.close();
            dParameters_.countAllNodes();
            dParameters_.countsToProbability();
            dParameters_.setAttrValues(attValues);
            dParameters_.saveModel(conn, pModel, pK);
           
            if (System.getProperty ("oracle.jserver.version") == null) {
                conn.close();
                System.out.println("Database connection closed");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving data! " + e.getMessage());
        }
        saveStats("Pass2", pModel);
    }
    
    /** Print a summary of the model */
    private static void printModel() {
        System.out.println(dParameters_);
    }

    /** Predict the class for a few records to verify results */
    private static void predict (String pQuery, int maxRows) {

        String[][] records;
        String[] attrKeys;
        HashMap<String, Integer> attrIndexes = new HashMap<String, Integer>();
        double[][] probs;
        Instance inst;
        PreparedStatement selectStmt;
        Connection conn;
        ResultSet rset;
        ResultSetMetaData mdata;
        int numCols;
        int numRows;
        
        int nc = dParameters_.getNumClasses();
        ArrayList[] attValues = dParameters_.getAttrValues();
        String[] attributes = dParameters_.getAttributes();
        int[] attrOrder;
        int labelIndex = dParameters_.getClassIndex();

        conn = openConnection();
        try {
            selectStmt = conn.prepareStatement(pQuery);
            rset = selectStmt.executeQuery();
            mdata = rset.getMetaData();
            numCols = mdata.getColumnCount();
            attrKeys = new String[numCols];
            for (int i = 0; i < numCols; i++) {
                attrKeys[i] = mdata.getColumnName(i + 1);
                attrIndexes.put(mdata.getColumnName(i + 1), i);
            }
            attrOrder = new int[attributes.length];
            for (int i = 0; i < attributes.length; i++) {
                try {
                    attrOrder[i] = attrIndexes.get(attributes[i]);
                } catch (NullPointerException e) { 
                    attrOrder[i] = -1;
                }
            }
            records = new String[maxRows][numCols];
            numRows = maxRows;
            for (int i = 0; i < maxRows; i++) {
                if (rset.next()) {
                    for (int j = 0; j < numCols; j++) {
                        records[i][j] = rset.getString(j + 1);
                    }
                } else {
                    numRows = i;
                    break;
                }
            }
            probs = new double[numRows][nc];
            for (int n = 0; n < numRows; n++) {
                inst = new Instance(records[n], attrOrder, attValues, labelIndex);
                probs[n] = predictOne(inst);
                System.out.print("Record " + n + " label: " + records[n][attrOrder[labelIndex]] +
                        " Class probs: [");
                for (int i = 0; i < nc; i++) {
                    System.out.printf("%.4f, ", probs[n][i]);
                }
                System.out.println("]");
            }
            if (System.getProperty ("oracle.jserver.version") == null) {
                conn.close();
                System.out.println("Database connection closed");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving data! " + e.getMessage());
        }
    }
    
    /** Predicts the class for an instance */
    private static double[] predictOne(Instance inst) {

        int nc = dParameters_.getNumClasses();
        String[] attributes = dParameters_.getAttributes();
        ArrayList[] attValues = dParameters_.getAttrValues();
        int[] m_Order = dParameters_.getOrder();
        double[] probs = new double[nc];

        for (int c = 0; c < nc; c++) {
            probs[c] = dParameters_.getClassProbabilities()[c];
            for (int u = 0; u < m_Order.length; u++) {
                int uval = inst.value(m_Order[u]);
                BayesNode wd = dParameters_.getBayesNode(inst, u);
                double prob = wd.getXYProbability((int)uval, c);
                probs[c] += prob;
            }
        }

        SUtils.normalizeInLogDomain(probs);
        SUtils.exp(probs);
        return probs;
    }
    
}
