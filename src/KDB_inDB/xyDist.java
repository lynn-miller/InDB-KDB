/**
 * Copied from Fupla xyDist.java class
 * Unused methods have been removed
 * Added methods to update distributions for fact and dimension records
 */
package KDB_inDB;

public class xyDist {

    private int[][] counts_;
    private int[] classCounts_;	

    private int N;
    private int n;
    private int nc;

    private int paramsPerAtt[];	

    public xyDist(int numInstances, int numAttributes, int numClasses, int classIndex, int[] numValues) {

        N = numInstances;
        n = numAttributes; // -1 is due to the class presence in numAttributes
        nc = numClasses;

        paramsPerAtt = new int[n];
            for (int u = 0; u < n; u++) {
                paramsPerAtt[u] = numValues[u];
            }

        classCounts_ = new int[nc];
        counts_ = new int[n][];

        for (int u1 = 0; u1 < n; u1++) {
            if (u1 != classIndex) {
                counts_[u1] = new int[paramsPerAtt[u1] * nc];
            }
        }	
    }

    public void update(Instance inst) {
        int x_C = (int) inst.classValue();
        classCounts_[x_C]++;		

        for (int u1 = 0; u1 < n; u1++) {
            if (u1 != inst.classIndex()) {
                int x_u1 = (int) inst.value(u1);
                int pos = x_u1*nc + x_C;
                counts_[u1][pos]++;
            }
        }
    }	

    /**
     * Updates counts for a fact table record
     * 
     * @param inst - fact attribute values
     * @param last - index of last fact attribute
     */
    public void updateFact(Instance inst, int last) {
        int x_C = (int) inst.classValue();
        classCounts_[x_C]++;		

        for (int u1 = 0; u1 < last; u1++) {
            if (u1 != inst.classIndex()) {
                int x_u1 = (int) inst.value(u1);
                int pos = x_u1*nc + x_C;
                counts_[u1][pos]++;
            }
        }
    }	

    /**
     * Updates counts for a dimension record
     * 
     * @param inst     - dimension attribute values
     * @param first    - index of first dimension attribute
     * @param last     - index of last dimension attribute
     * @param keyIndex - index of dimension FK attribute 
     */
    public void updateDim(Instance inst, int first, int last, int keyIndex) {
        int x_key = (int) inst.classValue();

        for (int u1 = first; u1 < last; u1++) {
            if (u1-first != inst.classIndex()) {
                int x_u1 = (int) inst.value(u1-first);
                for (int c=0; c < nc; c++) {
                    // add the key counts to the attribute counts
                    int pos = x_u1*nc + c;
                    int keyPos = x_key*nc + c;
                    counts_[u1][pos] += counts_[keyIndex][keyPos];
                }
            }
        }
    }	

    // count[A=v,Y=y]
    public int getCount(int u1, int u1val, int y) {
            //return counts_[a][v*noOfClasses_+y];
            int pos = u1val*nc + y;
            return counts_[u1][pos];
    }

    // count[A=v]
    public int getCount(int u1, int u1val) {
            int c = 0;
            for (int y = 0; y < nc; y++) {
                    int pos = u1val*nc + y;
                    c += counts_[u1][pos];
            }
            return c;
    }

    // count[Y=y]
    public int getClassCount(int y) { return classCounts_[y]; }

    public int getNoClasses() { return nc; }

    public int getNoAtts() { return n; }

    public int getNoCatAtts() { return n; }

    public int getNoData() { return N; }

    public void setNoData() { N++; }

    public int getNoValues(int u) { return paramsPerAtt[u]; }

    @Override
    public String toString() {
        String result = "";
        for (int i=0; i < counts_.length; i++) {
            result = result + "Attribute: " + i + " counts: ";
            if (counts_[i] != null) {
                for (int j=0; j < counts_[i].length; j++) {
                    result = result + counts_[i][j] + ", ";
                }
            }
            result = result + "\n";
        }
        return result;
    }
}
