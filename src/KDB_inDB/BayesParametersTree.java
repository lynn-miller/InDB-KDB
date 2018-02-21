/**
 * Copied from Fupla (https://github.com/nayyarzaidi/fupla) wdBayesParameterTree.java class
 * Unused methods have been removed
 * Removed Weka dependencies
 * Added constructor to create tree from database structure table
 * Added methods to save and load the model to/from the database
 */
package KDB_inDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class BayesParametersTree {
	
    private double[] parameters;
    private int np;

    private BayesNode[] bayesNode_;
    private int[] activeNumNodes;

    private int N;
    private int n;
    private int numCols;
    private int nc;

    private int[] m_ParamsPerAtt;

    private String[] attributes;
    private ArrayList<String>[] attrValues;
    private int classIndex;
    private int[] order;
    private int[][] parents;

    private double[] classCounts;
    private double[] classProbabilities;
    
    private int recNum;

    /**
     * Constructor for in-database models
     */
    public BayesParametersTree(Connection conn, String modelName) throws SQLException {
        // Retrieve the structure from the database - order by attribute MI and parent CMI rankings
        PreparedStatement selectStmt = conn.prepareStatement("select " +
                "       s1.kdb$attr_num, s1.kdb$attr_name, s1.kdb$attr_name2, s1.kdb$mi_rank, " +
                "       s1.kdb$cmi_rank, s1.kdb$num_values, s2.kdb$attr_num parent, " +
                "       nvl(max(s1.kdb$cmi_rank) over (partition by s1.kdb$attr_Name),0) numParents, " +
                "       max(s1.kdb$mi_rank) over () numAttrs, " +
                "       max(s1.kdb$attr_num + 1) over () numCols " +
                "from KDB$S_" + modelName + " s1, " +
                "     (select distinct kdb$attr_name, kdb$mi_rank, kdb$attr_num from KDB$S_" + modelName + ") s2 " +
                "where s1.kdb$attr_name2 = s2.kdb$attr_name (+) " +
                "order by s1.kdb$mi_rank, s1.kdb$cmi_rank");
        selectStmt.setFetchSize(1000);
        ResultSet rset = selectStmt.executeQuery();
        int rec = 0;
        // Process the structure records
        while (rset.next()) {
            int attrNum = rset.getInt(1);
            String attrName = rset.getString(2);
            int miRank = rset.getInt(4) - 1;
            int cmiRank = rset.getInt(5) - 1;
            int numValues = rset.getInt(6);
            int parent = rset.getInt(7);
            int numParents = rset.getInt(8);
            if (rec == 0) {
                // The first record - initialise the structures 
                this.n = rset.getInt(9); // + 1;
                numCols = rset.getInt(10);
                order = new int[n];
                attributes = new String[numCols];
                m_ParamsPerAtt = new int[numCols];
                parents = new int[n][];
            }
            if (miRank == 0 || (miRank > 0 && cmiRank <= 0)) {
                // A new attribute, save the details and 1st parent
                attributes[attrNum] = attrName;
                order[miRank] = attrNum;
                m_ParamsPerAtt[attrNum] = numValues;
                if (numParents > 0)
                    parents[miRank] = new int[numParents];
            }
            if (cmiRank >= 0)
                // Another parent for the attribute
                parents[miRank][cmiRank] = parent;
            if (miRank == -1) {
                // Either the class or an attribute to ignore
                attributes[attrNum] = attrName;
                m_ParamsPerAtt[attrNum] = numValues;
                if (rset.getString(3) != null) {    // .equals("KDB$CLASS")) {
                    // This is the class attribute
                    classIndex = attrNum;
                    nc = numValues;
                }
            }
            rec ++;
        }
        
        rset.close();
        selectStmt.close();

        activeNumNodes = new int[n];
        bayesNode_ = new BayesNode[n];
        for (int u = 0; u < n; u++) {
            bayesNode_[u] = new BayesNode();
            bayesNode_[u].init(nc, m_ParamsPerAtt[order[u]]);
        }

        classCounts = new double[nc];
        classProbabilities = new double[nc];
        
    }
    
    public BayesParametersTree(int n, int nc, int[] paramsPerAtt, int[] m_Order, int[][] m_Parents) {
        this.n = n;
        this.numCols = n;
        this.nc = nc;

        m_ParamsPerAtt = new int[n];
        for (int u = 0; u < n; u++) {
            m_ParamsPerAtt[u] = paramsPerAtt[u];
        }

        order = new int[n];
        parents = new int[n][];

        for (int u = 0; u < n; u++) {
            order[u] = m_Order[u];
        }

        activeNumNodes = new int[n];	

        for (int u = 0; u < n; u++) {
            if (m_Parents[u] != null) {
                parents[u] = new int[m_Parents[u].length];
                for (int p = 0; p < m_Parents[u].length; p++) {
                    parents[u][p] = m_Parents[u][p];
                }
            }
        }

        bayesNode_ = new BayesNode[n];
        for (int u = 0; u < n; u++) {
            bayesNode_[u] = new BayesNode();
            bayesNode_[u].init(nc, paramsPerAtt[m_Order[u]]);
        }

        classCounts = new double[nc];
        classProbabilities = new double[nc];
    }

    /* 
     * -----------------------------------------------------------------------------------------
     * Update count statistics that is:  relevant ***xyCount*** in every node
     * -----------------------------------------------------------------------------------------
     */

    public void unUpdate(Instance instance) {
        classCounts[(int) instance.classValue()]--;

        for (int u = 0; u < n; u++) {
            unUpdateAttributeTrie(instance, u, order[u], parents[u]);
        }

        N--;
    }

    public void unUpdateAttributeTrie(Instance instance, int i, int u, int[] lparents) {

        int x_C = (int) instance.classValue();
        int x_u = (int) instance.value(u);		

        bayesNode_[i].decrementXYCount(x_u, x_C);	

        if (lparents != null) {

            BayesNode currentdtNode_ = bayesNode_[i];

            for (int d = 0; d < lparents.length; d++) {
                int p = lparents[d];				

                int x_up = (int) instance.value(p);

                currentdtNode_.children[x_up].decrementXYCount(x_u, x_C);
                currentdtNode_ = currentdtNode_.children[x_up];
            }
        }
    }

    public void update(Instance instance) {
        classCounts[(int) instance.classValue()]++;

        for (int u = 0; u < n; u++) {
            updateAttributeTrie(instance, u, order[u], parents[u]);
        }

        N++;
    }

    public void updateAttributeTrie(Instance instance, int i, int u, int[] lparents) {

        int x_C = (int) instance.classValue();
        int x_u = (int) instance.value(u);		

        bayesNode_[i].incrementXYCount(x_u, x_C);	

        if (lparents != null) {

            BayesNode currentdtNode_ = bayesNode_[i];

            for (int d = 0; d < lparents.length; d++) {
                int p = lparents[d];

                if (currentdtNode_.att == -1 || currentdtNode_.children == null) {
                    currentdtNode_.children = new BayesNode[m_ParamsPerAtt[p]];
                    currentdtNode_.att = p;	
                }

                int x_up = (int) instance.value(p);
                currentdtNode_.att = p;

                // the child has not yet been allocated, so allocate it
                if (currentdtNode_.children[x_up] == null) {
                    currentdtNode_.children[x_up] = new BayesNode();
                    currentdtNode_.children[x_up].init(nc, m_ParamsPerAtt[u]);
                } 

                currentdtNode_.children[x_up].incrementXYCount(x_u, x_C);
                currentdtNode_ = currentdtNode_.children[x_up];
            }
        }
    }

    /* 
     * -----------------------------------------------------------------------------------------
     * Convert count into (NB) probabilities
     * -----------------------------------------------------------------------------------------
     */

    public void countsToProbability() {
        for (int c = 0; c < nc; c++) {
            classProbabilities[c] = Math.log(SUtils.MEsti(classCounts[c], N, nc));
        }
        for (int u = 0; u < n; u++) {
            convertCountToProbs(order[u], parents[u], bayesNode_[u]);
        }
    }

    public void convertCountToProbs(int u, int[] lparents, BayesNode pt) {

        int att = pt.att;

        if (att == -1) {
            for (int y = 0; y < nc; y++) {

                int denom = 0;
                for (int dval = 0; dval < m_ParamsPerAtt[u]; dval++) {
                    denom += pt.getXYCount(dval, y);
                }

                for (int uval = 0; uval < m_ParamsPerAtt[u]; uval++) {
                    double prob = Math.log(Math.max(SUtils.MEsti(pt.getXYCount(uval, y), denom, m_ParamsPerAtt[u]),1e-75));
                    pt.setXYProbability(uval, y, prob);
                }
            }			
            return;
        }

        while (att != -1) {
            /*
             * Now convert non-leaf node counts to probs
             */
            for (int y = 0; y < nc; y++) {

                int denom = 0;
                for (int dval = 0; dval < m_ParamsPerAtt[u]; dval++) {
                    denom += pt.getXYCount(dval, y);
                }

                for (int uval = 0; uval < m_ParamsPerAtt[u]; uval++) {
                    double prob = Math.log(Math.max(SUtils.MEsti(pt.getXYCount(uval, y), denom, m_ParamsPerAtt[u]),1e-75));
                    pt.setXYProbability(uval, y, prob);
                }
            }

            int numChildren = pt.children.length;
            for (int c = 0; c < numChildren; c++) {
                BayesNode next = pt.children[c];
                if (next != null) 					
                    convertCountToProbs(u, lparents, next);

                // Flag end of nodes
                att = -1;				
            }			
        }

        return;
    }

    //probability when using leave one out cross validation, the t value is discounted
    public double ploocv(int y, int x_C) {
        if (y == x_C)
            return SUtils.MEsti(classCounts[y] - 1, N - 1, nc);
        else
            return SUtils.MEsti(classCounts[y], N - 1, nc);
    }

    public void updateClassDistributionloocv(double[][] classDist, int i, int u, Instance instance, int m_KDB) {

        int x_C = (int) instance.classValue();
        int uval = (int) instance.value(u);

        BayesNode pt = bayesNode_[i];
        int att = pt.att;

        // find the appropriate leaf
        int depth = 0;
        while ( (att != -1)) { //We want to consider kdb k=k

            // sum over all values of the Attribute for the class to obtain count[y, parents]
            for (int y = 0; y < nc; y++) {
                int totalCount = (int) pt.getXYCount(0, y);
                for (int val = 1; val < m_ParamsPerAtt[u]; val++) {
                    totalCount += pt.getXYCount(val, y);
                }    

                if (y != x_C)
                    classDist[depth][y] *= SUtils.MEsti(pt.getXYCount(uval, y), totalCount, m_ParamsPerAtt[u]);
                else
                    classDist[depth][y] *= SUtils.MEsti(pt.getXYCount(uval, y)-1, totalCount-1, m_ParamsPerAtt[u]);
            }

            int v = (int) instance.value(att);

            BayesNode next = pt.children[v];
            if (next == null) {
                for (int k = depth + 1; k <= m_KDB; k++) {
                    for (int y = 0; y < nc; y++) 
                        classDist[k][y] = classDist[depth][y];
                }
                return;
            };

            // check that the next node has enough examples for this value;
            int cnt = 0;
            for (int y = 0; y < nc; y++) {
                cnt += next.getXYCount(uval, y);
            }

            //In loocv, we consider minCount=1(+1), since we have to leave out i.
            if (cnt < 2) { 
                depth++;
                // sum over all values of the Attribute for the class to obtain count[y, parents]
                for (int y = 0; y < nc; y++) {
                    int totalCount = (int) pt.getXYCount(0, y);
                    for (int val = 1; val < m_ParamsPerAtt[u]; val++) {
                        totalCount += pt.getXYCount(val, y);
                    }    

                    if (y != x_C)
                        classDist[depth][y] *= SUtils.MEsti(pt.getXYCount(uval, y), totalCount, m_ParamsPerAtt[u]);
                    else
                        classDist[depth][y] *= SUtils.MEsti(pt.getXYCount(uval, y)-1, totalCount-1, m_ParamsPerAtt[u]);
                }

                for (int k = depth + 1; k <= m_KDB; k++){
                    for (int y = 0; y < nc; y++) 
                        classDist[k][y] = classDist[depth][y];
                }
                return;
            }

            pt = next;
            att = pt.att; 
            depth++;
        }

        // sum over all values of the Attribute for the class to obtain count[y, parents]
        for (int y = 0; y < nc; y++) {
            int totalCount = (int) pt.getXYCount(0, y);
            for (int val = 1; val < m_ParamsPerAtt[u]; val++) {
                totalCount += pt.getXYCount(val, y);
            }    
            if (y != x_C)
                classDist[depth][y] *=  SUtils.MEsti(pt.getXYCount(uval, y), totalCount, m_ParamsPerAtt[u]);
            else
                classDist[depth][y] *=  SUtils.MEsti(pt.getXYCount(uval, y)-1, totalCount-1, m_ParamsPerAtt[u]);
        }

        for (int k = depth + 1; k <= m_KDB; k++){
            for (int y = 0; y < nc; y++) 
                classDist[k][y] = classDist[depth][y];
        }

    }

    public void updateClassDistributionloocv2(double[][] posteriorDist, int i, int u, Instance instance, int m_KDB) {

        int x_C = (int) instance.classValue();

        BayesNode pt = bayesNode_[i];
        int att = pt.att;

        int noOfVals = m_ParamsPerAtt[u];
        int targetV = (int) instance.value(u);

        // find the appropriate leaf
        int depth = 0;
        while (att != -1) { // we want to consider kdb k=k
            for (int y = 0; y < nc; y++) {
                if (y != x_C)
                    posteriorDist[depth][y] *= SUtils.MEsti(pt.getXYCount(targetV, y), classCounts[y], noOfVals);
                else
                    posteriorDist[depth][y] *= SUtils.MEsti(pt.getXYCount(targetV, y) - 1, classCounts[y]-1, noOfVals);
            }

            int v = (int) instance.value(att);

            BayesNode next = pt.children[v];
            if (next == null) 
                break;

            // check that the next node has enough examples for this value;
            int cnt = 0;
            for (int y = 0; y < nc && cnt < 2; y++) {
                cnt += next.getXYCount(targetV, y);
            }

            // In loocv, we consider minCount=1(+1), since we have to leave out i.
            if (cnt < 2){ 
                depth++;
                break;
            }

            pt = next;
            att = pt.att;
            depth++;
        } 

        for (int y = 0; y < nc; y++) {
            double mEst;
            if (y != x_C)
                mEst = SUtils.MEsti(pt.getXYCount(targetV, y), classCounts[y], noOfVals);
            else
                mEst = SUtils.MEsti(pt.getXYCount(targetV, y)-1, classCounts[y]-1, noOfVals);

            for (int k = depth; k <= m_KDB; k++){
                posteriorDist[k][y] *= mEst;
            }
        }

    }	

    public BayesNode getBayesNode(Instance instance, int i, int u, int[] m_Parents) {	

        BayesNode pt = bayesNode_[i];
        int att = pt.att;

        // find the appropriate leaf
        while (att != -1) {
            int v = (int) instance.value(att);
            BayesNode next = pt.children[v];
            if (next == null) 
                break;
            pt = next;
            att = pt.att;
        }

        return pt;		
    }

    public BayesNode getBayesNode(Instance instance, int i) {	

        BayesNode pt = bayesNode_[i];
        int att = pt.att;

        // find the appropriate leaf
        while (att != -1 && instance.value(att) != -1) {
            int v = (int) instance.value(att);
            BayesNode next = pt.children[v];
            if (next == null) 
                break;
            pt = next;
            att = pt.att;
        }
//        System.out.println(pt.toString(""));
//        System.out.println("----------");
        return pt;		
    }

    public void cleanUp(int m_BestattIt, int m_BestK_) {

        for (int i = m_BestattIt; i < n; i++) {
            bayesNode_[i] = null;
        }

        for (int i = 0; i < m_BestattIt; i++) {
            if (parents[i] != null) {
                if (parents[i].length > m_BestK_) {
                    int level = -1;
                    deleteExtraNodes(bayesNode_[i], m_BestK_, level);
                }	
            }
        }
    }

    public void deleteExtraNodes(BayesNode pt, int k, int level) {

        level = level + 1;

        int att = pt.att;

        while (att != -1) {

            int numChildren = pt.children.length;
            for (int c = 0; c < numChildren; c++) {
                BayesNode next = pt.children[c];

                if (level == k) {
                    pt.children[c] = null;
                    pt.att = -1;
                    next = null;
                }

                if (next != null) 
                    deleteExtraNodes(next, k, level);

                att = -1;
            }
        }

    }

    /* 
     * -----------------------------------------------------------------------------------------
     * Allocate Parameters
     * -----------------------------------------------------------------------------------------
     */	

    public void allocate() {
            // count active nodes in Trie
        np = nc;
        for (int u = 0; u < n; u++) {
            BayesNode pt = bayesNode_[u];
            activeNumNodes[u] = countActiveNumNodes(u, order[u], parents[u], pt);
        }		
        System.out.println("Allocating dParameters of size: " + np);
        parameters = new double[np];				
    }

    public void countAllNodes() {
            // count active nodes in Trie
        for (int u = 0; u < n; u++) {
            BayesNode pt = bayesNode_[u];
            activeNumNodes[u] = countActiveNumNodes(u, order[u], parents[u], pt);
        }		
    }

    public int countActiveNumNodes(int i, int u, int[] lparents, BayesNode pt) {
        int numNodes = 0;		
        int att = pt.att;

        if (att == -1) {
//	      pt.index = np;
            np += m_ParamsPerAtt[u] * nc;			
            return 1;			
        }			

        while (att != -1) {
            int numChildren = pt.children.length;
            for (int c = 0; c < numChildren; c++) {
                BayesNode next = pt.children[c];
                if (next != null)
                    numNodes += countActiveNumNodes(i, u, lparents, next);
                att = -1;
            }			
        }

        return numNodes;
    }

    /* 
     * -----------------------------------------------------------------------------------------
     * xyParameters to Parameters
     * -----------------------------------------------------------------------------------------
     */	

    public double[] getParameters() {
        return parameters;
    }

    public int getNp() {
        return np;
    }

    public int setNAttributes(int newn) {
        return n = newn;
    }

    public int getNAttributes() {
        return n;
    }

    public int getNumCols() {
        return numCols;
    }

    public int getClassIndex() {
        return classIndex;
    }

    public void setClassIndex(int classIndex) {
        this.classIndex = classIndex;
    }

    public double[] getClassCounts() {
        return classCounts;
    }

    public double[] getClassProbabilities() {
        return classProbabilities;
    }

    public void setAttrValues(ArrayList[] attrValues) {
        this.attrValues = attrValues;
    }

    public ArrayList[] getAttrValues() {
        return attrValues;
    }

    public int getNumClasses() {
        return nc;
    }

    public String[] getAttributes() {
        return attributes;
    }

    public void setAttributes(String[] attributes) {
        this.attributes = attributes;
    }

    public int[] getOrder() {
        return order;
    }

    /**
     * Method to save the model to the database
     * 
     * @param conn      - database connection
     * @param modelName - name of saved model
     * @param K         - number of parent attributes
     * @throws SQLException 
     */
    public void saveModel(Connection conn, String modelName, int K) throws SQLException {
        String tableName = "KDB$M_" + modelName;
        PreparedStatement preparedStmt;

        try {
            preparedStmt = conn.prepareStatement("drop table " + tableName + " purge");
            preparedStmt.execute();
            preparedStmt.close();
        } catch (SQLException e) {}
        String query = "create table " + tableName + "( " +
                "kdb$attr_name   varchar2(30), " +
                "kdb$attr_value  varchar2(4000), ";
        for (int k=1; k<=K; k++) {
            query = query + "kdb$parent" + k + " varchar2(4000), ";
        }
        query = query + "kdb$class  varchar2(4000), " +
                "kdb$count          number(38), " +
                "kdb$group_id       number(38), " +
                "kdb$num_values     number(38), " +
                "kdb$parents_count  number(38)) pctfree 0 nologging";
        preparedStmt = conn.prepareStatement(query);
        preparedStmt.execute();
        preparedStmt.close();
        recNum = 0;
        query = "insert /*+ append_values */ into " + tableName + 
                "(kdb$attr_name, kdb$attr_value, kdb$class, kdb$count, kdb$group_id, kdb$num_values, kdb$parents_count";
        for (int k = 1; k <= K; k++) query = query + ", kdb$parent" + k;
        query = query + ") values (?, ?, ?, ?, ?, ?, ?";
        for (int k = 1; k <= K; k++) query = query + ", ?";
        query = query + ")";
        preparedStmt = conn.prepareStatement(query);
        for (int c=0; c<nc; c++) {
            preparedStmt.setString(1, null);
            preparedStmt.setString(2, null);
            preparedStmt.setString(3, attrValues[classIndex].get(c));
            preparedStmt.setDouble(4, classCounts[c]);
            preparedStmt.setInt(5, 0);
            preparedStmt.setInt(6, nc);
            preparedStmt.setInt(7, N);
            for (int k = 1; k <= K; k++) preparedStmt.setString(k+7, null);
            insertRecord(conn, preparedStmt);
        }
        for (int u = 0; u < n; u++) {
            if (order[u] != classIndex && bayesNode_[u] != null) {
                BayesNode pt = bayesNode_[u];
                saveNode(conn, preparedStmt, tableName, K, null, attributes[order[u]], order[u], pt);
            }
        }
        if (recNum != 0 ) {
            int[] result = preparedStmt.executeBatch();
//            System.out.println("Model rows: " + result.length);
            conn.commit();
            recNum = 0;
        }
        preparedStmt.close();
    }
    
    // Helper method - saves a record and commits after every 10,000 records
    public void insertRecord(Connection conn, PreparedStatement stmt) throws SQLException {
        stmt.addBatch();
        recNum = recNum + 1;
        if (recNum >= 10000) {
            int[] result = stmt.executeBatch();
//            System.out.println("Model rows: " + result.length);
            conn.commit();
            recNum = 0;
        }
    }

    // Helper method - creates the records for each node in the parameter tree
    public void saveNode(Connection conn, PreparedStatement stmt, String modelName, int K, String[] parentValues, String attrName, int attrNum, BayesNode pt) throws SQLException {

        int nv = attrValues[attrNum].size(); 
        int np = (parentValues == null) ? 0 : parentValues.length;
        int parentsCount = 0;
        
        for (int c = 0; c < nc; c++) {
            parentsCount = 0;
            for (int v = 0; v < nv; v++) parentsCount += pt.getXYCount(v, c);
            for (int v = 0; v < nv; v++) {
                stmt.setString(1, attrName);
                stmt.setString(2, attrValues[attrNum].get(v));
                stmt.setString(3, attrValues[classIndex].get(c));
                stmt.setDouble(4, pt.getXYCount(v, c));
                stmt.setInt(5, np);
                stmt.setInt(6, nv);
                stmt.setInt(7, parentsCount);
                for (int k = 0; k < np; k++) stmt.setString(k+8, parentValues[k]);
                for (int k = np; k < K; k++) stmt.setString(k+8, null);
                insertRecord(conn, stmt);
            }
        }
        String[] nextValues = new String[np+1];
        for (int p=0; p < np; p++) nextValues[p] = parentValues[p];
        if (pt.children != null && pt.att != -1) {
            for (int u = 0; u < pt.children.length; u++) {
                nextValues[np] = attrValues[pt.att].get(u);
                BayesNode next = pt.children[u];
                if (next != null) saveNode(conn, stmt, modelName, K, nextValues, attrName, attrNum, next);
            }		
        }
    }

    /**
     * Method to load the model from the database
     * 
     * @param conn      - database connection
     * @param modelName - name of model to load
     * @param K         - number of parent attributes
     * @throws SQLException 
     */
    public void loadModel(Connection conn, String modelName, int K) throws SQLException {
        String tableName = "KDB$M_" + modelName;
        String structure = "KDB$S_" + modelName;
        PreparedStatement preparedStmt;
        attrValues = new ArrayList[numCols];
        for (int i = 0; i < numCols; i++) attrValues[i] = new ArrayList(m_ParamsPerAtt[i]);
        String query = "select s.kdb$attr_num, s.kdb$mi_rank, " +
                "t.kdb$attr_name, t.kdb$attr_value, t.kdb$class, t.kdb$count, t.kdb$group_id"; //, t.kdb$num_values, t.kdb$parents_count";
        for (int k = 1; k <= K; k++) query = query + ", kdb$parent" + k;
        query = query + " from " + tableName + " t, " +
                "(select distinct kdb$attr_num, kdb$attr_name, kdb$mi_rank from " + structure + ") s " +
                "where t.kdb$attr_name = s.kdb$attr_name (+)";
        preparedStmt = conn.prepareStatement(query);
        preparedStmt.setFetchSize(1000);
        System.out.println(query);
        ResultSet rset = preparedStmt.executeQuery();
        while (rset.next()) {
            int attrNum = rset.getInt(1);
            int attrRank = rset.getInt(2) - 1;
            String attrName = rset.getString(3);
            String attrValue = rset.getString(4);
            String classValue = rset.getString(5);
            int count = rset.getInt(6);
            int numParents = rset.getInt(7);
            String[] parentValues = new String[numParents];
            for (int k = 0; k < numParents; k++) parentValues[k] = rset.getString(k+8);
            int clsInd = attrValues[classIndex].indexOf(classValue);
            if (clsInd == -1) {
                clsInd = attrValues[classIndex].size();
                attrValues[classIndex].add(classValue);
            }
            if (attrName == null) {
                classCounts[clsInd] = count;
                N += count;
            } else {
                int attInd = attrValues[attrNum].indexOf(attrValue);
                if (attInd == -1) {
                    attInd = attrValues[attrNum].size();
                    attrValues[attrNum].add(attrValue);
                }
                BayesNode pt = bayesNode_[attrRank];
                BayesNode oldPt = pt;
                for (int k=0; k < numParents; k++) {
                    int parNum = parents[attrRank][k];
                    int parInd = attrValues[parNum].indexOf(parentValues[k]);
                    if (parInd == -1) {
                        parInd = attrValues[parNum].size();
                        attrValues[parNum].add(parentValues[k]);
                    }
                    if (pt.att == -1 || pt.children == null) {
                        pt.children = new BayesNode[m_ParamsPerAtt[parNum]];
                        pt.att = parNum;	
                    }

                    if (pt.children[parInd] == null) {
                        pt.children[parInd] = new BayesNode();
                        pt.children[parInd].init(nc, m_ParamsPerAtt[attrNum]);
                    }
                    oldPt = pt;
                    pt = pt.children[parInd];
                }
                pt.setXYCount(attInd, clsInd, count);
            }
        }
    }

    @Override
    public String toString() {
        String result = "\n--- Start of Model ---\n";
        result = result + "Training instances: " + N + "\n";
        result = result + "Attributes: " + Arrays.toString(attributes) + "\n";
        result = result + "Predictors: " + Arrays.toString(order) + "\n";
        result = result + "Label column: " + attributes[classIndex] + "; index: " + classIndex + "\n";
        result = result + "Classes: " + attrValues[classIndex] + "\n";
        result = result + "Class Probabilities: ";
        for (int i=0; i<classProbabilities.length; i++) {
            result = result + String.format("%.4f, ", Math.exp(classProbabilities[i]));
        }
        result = result + String.format("\nObservations:%d; Attributes:%d; Classes:%d; \nTree nodes(%d): ",
                N, n, nc, activeNumNodes.length);
        for (int i=0; i<activeNumNodes.length; i++)
            result = result + activeNumNodes[i] + ", ";
        result = result + "\n---- End of Model ----\n";
        return result;
    }
}