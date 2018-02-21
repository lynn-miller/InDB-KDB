/**
 * Adapted from Fupla (https://github.com/nayyarzaidi/fupla) fupla.java class
 */
package KDB_inDB;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * In-database KDB - inDB-Java1 method
 * Pass 1 is inDB-Java1 - Uses ArrayMaps to hold MI and CMI counts
 * Pass 2 is not used in results
 * 
 * @author lynnm
 */
public class KDBIDB2SQL { 

//  Connection information for OOD testing    
//    private String m_DB = "localhost:1521:XE";
    private String m_DB = "rhino.its.monash.edu.au:1521:EDUDB";
    private String m_User = "bayes";
    private String m_Password = "";

//  Default parameters
    private int m_K = 2;
    private int m_EstClasses = 16;
    private int m_EstValues = 16;
    private String m_Query = "select * from nursery_input";
    private String m_Label = "OUTCOME";
    private boolean m_Trace = false;
    private String m_Model = "unnamed";
    
    String[] attributes;
    ArrayList[] attValues;
    double[] classProbabilities;
    double[][][] condProbs;
    int labelIndex = -1;
    int nc;
    int N;

    int[][] m_Parents;
    int[] m_Order;
    
    BayesParametersTree dParameters_;

    /** Get CPU time in nanoseconds. */
    public long getCpuTime( ) {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
        return bean.isCurrentThreadCpuTimeSupported( ) ?
            bean.getCurrentThreadCpuTime( ) : 0L;
    }

    /** Get user time in nanoseconds. */
    public long getUserTime( ) {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
        return bean.isCurrentThreadCpuTimeSupported( ) ?
            bean.getCurrentThreadUserTime( ) : 0L;
    }

    /** Get system time in nanoseconds. */
    public long getSystemTime( ) {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
        return bean.isCurrentThreadCpuTimeSupported( ) ?
            (bean.getCurrentThreadCpuTime( ) - bean.getCurrentThreadUserTime( )) : 0L;
    }

    /** Retrieve the connection when using in-database JVM
     *   Else connect to the database using connect string, user name and password
     */
    public Connection openConnection() {
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
     * @param args are:
     *    K - the maximum number of parents
     *    L - the label (class) attribute
     *    M - the model name
     *    Q - the query returning the training data
     *    C - estimate of the number of classes
     *        used for initial sizing of arrayMaps
     *    V - estimate of the number of attribute values
     *        used for initial sizing of arrayMaps
     *    T - switch to enable SQL tracing
     */
    public void setOptions(String[] args) throws Exception {
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
            } else if (option.equals("C")) {
                m_EstClasses = Integer.parseInt(thisParam);
            } else if (option.equals("V")) {
                m_EstValues = Integer.parseInt(thisParam);
            } else if (option.equals("T")) {
                m_Trace = true;
            }
        }
        System.out.printf("K:%d;L:%s;Q:%s\n", m_K, m_Label, m_Query);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        KDBIDB2SQL kdb = new KDBIDB2SQL();
        try {
            kdb.setOptions(args);
            kdb.buildModel();
            kdb.printModel();
//            kdb.predict(100);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    /**
     * Calculate MIs from counts
     * Copied from Fupla CorrelationMeasures.java class getMutualInformation method
     * with minor alterations
     */
    private void getMutualInformation(ArrayList attValues[],
                                      ArrayList<ArrayList<Integer>> attCounts[],
                                      double[] mi) {
        
        double m, avCount, yCount;
        double[] avyCount = new double[nc];

        for (int u = 0; u < attributes.length; u++) {
            if (u != labelIndex) {
                m = 0;
                for (int uval = 0; uval < attValues[u].size(); uval++) {
                    avCount = 0;
                    for (int y = 0; y < attCounts[u].get(uval).size(); y++) {
                        if ( y < attCounts[u].get(uval).size() ) {
                            avyCount[y] = attCounts[u].get(uval).get(y);
                            avCount += avyCount[y];
                        } else avyCount[y] = 0;
                    }
                    for (int y = 0; y < nc; y++) {
                        if (avyCount[y] > 0) {
                            yCount = attCounts[labelIndex].get(0).get(y);
                            m += (avyCount[y] / N) * Math.log( avyCount[y] / ( avCount/N * yCount ) ) / Math.log(2);
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
    private void getCMI(ArrayList attValues[],
                        ArrayList<ArrayList<Integer>> xyCounts[],
                        ArrayList<ArrayList<ArrayList<Integer>>> xxyCounts[][],
                        double[][] cmi) {
        
        double a, b, d, e, mi, mitemp;

        for (int u1 = 1; u1 < attributes.length; u1++) {
            for (int u2 = 0; u2 < u1; u2++) {
                if (u1 != labelIndex && u2 != labelIndex ) {
                    mi = 0;
                    for (int u1val = 0; u1val < attValues[u1].size(); u1val++) {
                        for (int u2val = 0; u2val < attValues[u2].size(); u2val++) {
                            for (int c = 0; c < xxyCounts[u1][u2].get(u1val).get(u2val).size(); c++) {		
                                a = xxyCounts[u1][u2].get(u1val).get(u2val).get(c);
                                if (a != 0) {															
                                    b = xyCounts[labelIndex].get(0).get(c);
                                    d = xyCounts[u1].get(u1val).get(c);
                                    e = xyCounts[u2].get(u2val).get(c);
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
    
    /** Build the KDB model */
    private void buildModel() {

        int thisIndex = -1;
        int classIndex = -1;
        int[] attIndexes;
        ArrayList<ArrayList<Integer>>[] attCounts;
        ArrayList<ArrayList<ArrayList<Integer>>>[][] xxyCounts;
        Integer currValue;

        PreparedStatement selectStmt;
        Connection conn;
        String thisValue;
        ResultSet rset;
        ResultSetMetaData mdata;
        int numCols;
        
        System.gc();
        long startCpuTimeNano = getCpuTime();
        long startUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        conn = openConnection();
        try {
            conn.prepareCall("alter system flush buffer_cache").execute();
            System.out.println("begin proc_save_java_stats('Java-SQL-2','" + m_Model + "','Start'," +
                    startCpuTimeNano + "," + startUsedMem + "); end;");
            conn.prepareCall("begin proc_save_java_stats('Java-SQL-2','" + m_Model + "','Start'," +
                    startCpuTimeNano + "," + startUsedMem + "); end;").execute();

            // Run the query to get the training data
            selectStmt = conn.prepareStatement(m_Query);
            selectStmt.setFetchSize(1000);
            rset = selectStmt.executeQuery();

            // Process the query metadata to get attribute names
            mdata = rset.getMetaData();
            numCols = mdata.getColumnCount();
            attributes = new String[numCols];
            for (int i = 0; i < numCols; i++) {
                attributes[i] = mdata.getColumnName(i + 1);
            }
            attValues = new ArrayList[attributes.length];
            attIndexes = new int[attributes.length];
            attCounts = new ArrayList[attributes.length];
            xxyCounts = new ArrayList[attributes.length][attributes.length];
            for (int i = 0; i < attributes.length; i++) {
                attValues[i] = new ArrayList();
                attCounts[i] = new ArrayList<ArrayList<Integer>>(m_EstValues);
                for (int j = 0; j < attributes.length; j++) {
                    xxyCounts[i][j] = new ArrayList<ArrayList<ArrayList<Integer>>>(m_EstValues);
                }
                if (attributes[i].equals(m_Label)) labelIndex = i;
            }
            attCounts[labelIndex].add(new ArrayList<Integer>(m_EstClasses));

            // Process each training record and increment counts
            while (rset.next()) {
                // For each attribute, get the value index
                for (int i = 0; i < attributes.length; i++) {
                    thisValue = rset.getString(i + 1);
                    thisIndex = attValues[i].indexOf(thisValue);
                    // If a new value create the ArrayMaps for MI and CMI counts
                    if ( thisIndex == -1 ) {
                        thisIndex = attValues[i].size();
                        attValues[i].add(thisValue);
                        if ( i == labelIndex ) {
                            m_EstClasses = Math.max(m_EstClasses,attValues[labelIndex].size());
                        } else {
                            attCounts[i].add(new ArrayList<Integer>(m_EstClasses));
                            for (int j = 0; j < attributes.length; j++) {
                                if ( j != labelIndex ) {
                                    if (j < i) {
                                        xxyCounts[i][j].add(new ArrayList<ArrayList<Integer>>(Math.max(m_EstValues,attValues[j].size())));
                                        for (int k = 0; k < attValues[j].size(); k++) {
                                            xxyCounts[i][j].get(thisIndex).add(new ArrayList<Integer>(m_EstClasses));
                                        }
                                    } else if (j > i) {
                                        for (int k = 0; k < attValues[j].size(); k++) {
                                            xxyCounts[j][i].get(k).add(new ArrayList<Integer>(m_EstClasses));
                                        }
                                    }
                                }
                            }
                        }
                    } 
                    if ( i == labelIndex ) classIndex = thisIndex;
                    attIndexes[i] = thisIndex;
                } 

                for (int i = 0; i < attributes.length; i++) {
                    // For each attribute, increment the attribute value counts
                    thisIndex = (i == labelIndex) ? 0 : attIndexes[i];
                    for (int j = attCounts[i].get(thisIndex).size(); j <= classIndex; j++) {
                        attCounts[i].get(thisIndex).add(0);
                    }
                    currValue = attCounts[i].get(thisIndex).get(classIndex);
                    attCounts[i].get(thisIndex).set(classIndex, currValue + 1);

                    // For each pair of attributes, increment the CMI counts
                    if ( i != labelIndex ) {
                        for (int j = 0; j < i; j++) {
                            if ( j != labelIndex ) {
                                for (int k = xxyCounts[i][j].get(attIndexes[i]).get(attIndexes[j]).size(); k <= attIndexes[labelIndex]; k++) {
                                    xxyCounts[i][j].get(attIndexes[i]).get(attIndexes[j]).add(0);
                                }
                                currValue = xxyCounts[i][j].get(attIndexes[i]).get(attIndexes[j]).get(classIndex);
                                xxyCounts[i][j].get(attIndexes[i]).get(attIndexes[j]).set(classIndex, currValue + 1);
                            }
                        }
                    }
                }
            } 
            rset.close();
            selectStmt.close();

            // Calculate the MIs and CMIs
            nc = attValues[labelIndex].size();
            N = 0;
            for (int c = 0; c < nc; c++) {
		N = N + attCounts[labelIndex].get(0).get(c);
            }
            double[] mi = new double[attributes.length];
            getMutualInformation(attValues, attCounts, mi);
            double[][] cmi = new double[attributes.length][attributes.length];
            getCMI(attValues, attCounts, xxyCounts, cmi);
            
            // Sort attributes on MI with the class
            m_Order = SUtils.sort(mi);
            m_Parents = new int[attributes.length][];

            // Calculate parents based on MI and CMI
            for (int u = 0; u < attributes.length - 1; u++) {
                int nK = Math.min(u, m_K);
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

            // Print the structure
            System.out.println("Attribute order:" + Arrays.toString(m_Order));
            
            System.gc();
            long pass1CpuTimeNano = getCpuTime();
            long pass1UsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            conn.prepareCall("begin proc_save_java_stats('Java-SQL-2','" + m_Model + "','Pass1'," +
                    pass1CpuTimeNano + "," + pass1UsedMem + "); end;").execute();

            xxyCounts = null;
            attCounts = null;

            /*
             * ------------------------------------------------------
             * Pass No. 2
             * ------------------------------------------------------ 
             */
            conn.prepareCall("alter system flush buffer_cache").execute();
            int[] paramsPerAtt = new int[attributes.length];
            for (int i = 0; i < attributes.length; i++) {
                paramsPerAtt[i] = attValues[i].size();
            }
            dParameters_ = new BayesParametersTree(attributes.length, nc, paramsPerAtt, m_Order, m_Parents);
            selectStmt = conn.prepareStatement(m_Query);
            selectStmt.setFetchSize(1000);
            rset = selectStmt.executeQuery();
            while (rset.next()) {
                Instance row = new Instance(rset, attValues, labelIndex, true, null);
                dParameters_.update(row);
            }
            rset.close();
            selectStmt.close();
            dParameters_.countsToProbability();
            dParameters_.setAttrValues(attValues);
            dParameters_.setAttributes(attributes);
            dParameters_.setClassIndex(labelIndex);
            dParameters_.saveModel(conn, m_Model, m_K);
           
            System.gc();
            long pass2CpuTimeNano = getCpuTime();
            long pass2UsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            conn.prepareCall("begin proc_save_java_stats('Java-SQL-2','" + m_Model + "','Pass2'," +
                    pass2CpuTimeNano + "," + pass2UsedMem + "); end;").execute();

            if (System.getProperty ("oracle.jserver.version") == null) {
                conn.close();
                System.out.println("Database connection closed");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving data! " + e.getMessage());
        }
    }
    
    /** Print a summary of the model */
    private void printModel() {
        System.out.println("\n--- Start of Model ---");
        System.out.println("Training instances: " + N);
        System.out.println("Attributes: " + Arrays.toString(attributes));
        System.out.println("Label column: " + m_Label + "; index: " + labelIndex);
        System.out.println("Classes: " + attValues[labelIndex]);
        System.out.println(dParameters_);
        System.out.println("---- End of Model ----\n");
    }

    /** Predict the class for a few records to verify results */
    private void predict (int maxRows) {

        String [][] records;
        String[] attrKeys;
        double[][] probs;
        Instance inst;
        PreparedStatement selectStmt;
        Connection conn;
        ResultSet rset;
        ResultSetMetaData mdata;
        int numCols;
        int numRows;

        conn = openConnection();
        try {
            selectStmt = conn.prepareStatement(m_Query);
            rset = selectStmt.executeQuery();
            mdata = rset.getMetaData();
            numCols = mdata.getColumnCount();
            attrKeys = new String[numCols];
            for (int i = 0; i < numCols; i++) {
                attrKeys[i] = mdata.getColumnName(i + 1);
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
                inst = new Instance(records[n], attValues, labelIndex);
                probs[n] = predictOne(inst);
                System.out.print("Record " + n + " label: " + records[n][labelIndex] +
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
    private double[] predictOne(Instance inst) {

        double[] probs = new double[nc];

        for (int c = 0; c < nc; c++) {
            probs[c] = dParameters_.getClassProbabilities()[c];
            for (int u = 0; u < attributes.length - 1; u++) {
                double uval = inst.value(m_Order[u]);
                BayesNode wd = dParameters_.getBayesNode(inst, u);
                probs[c] += wd.getXYProbability((int)uval, c);
            }
        }

        SUtils.normalizeInLogDomain(probs);
        SUtils.exp(probs);
        return probs;
    }
    
}
