/**
 * Copied from Fupla xxyDist.java class
 * Unused methods have been removed
 * Added methods to update distributions for fact and dimension records
 */
package KDB_inDB;

public class xxyDist {

    private double[][][] counts_;

    public xyDist xyDist_;

    private int N;
    private int n;
    private int nc;

    private int paramsPerAtt[];

    public xxyDist(int numInstances, int numAttributes, int numClasses, int classIndex, int[] numValues) {

        N = numInstances;
        n = numAttributes; // -1 is due to the class presence in numAttributes
        nc = numClasses;

        paramsPerAtt = new int[n];
        for (int u = 0; u < n; u++) {
            paramsPerAtt[u] = numValues[u];
        }

        xyDist_ = new xyDist(N, n, nc, classIndex, paramsPerAtt);		
        counts_ = new double[n][][];

        for (int u1 = 1; u1 < n; u1++) {
            if (u1 != classIndex) {
                counts_[u1] = new double[paramsPerAtt[u1] * u1][];

                for (int u1val = 0; u1val < paramsPerAtt[u1]; u1val++) {
                    for (int u2 = 0; u2 < u1; u2++) {
                        if (u2 != classIndex) {
                            int pos1 = u1*u1val + u2;
                            counts_[u1][pos1] = new double[paramsPerAtt[u2] * nc];
                        }
                    }
                }
            }
        }
    }

    public void update(Instance inst) {		
        xyDist_.update(inst);

        int x_C = inst.classValue();

        for (int u1 = 1; u1 < n; u1++) {
            if (u1 != inst.classIndex()) {
                int x_u1 = inst.value(u1);

                for (int u2 = 0; u2 < u1; u2++) {
                    if (u2 != inst.classIndex()) {
                        int x_u2 = inst.value(u2);

                        int pos1 = u1*x_u1 + u2;
                        int pos2 = x_u2*nc + x_C; 
                        counts_[u1][pos1][pos2]++;
                    }
                }
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
        xyDist_.updateFact(inst, last);

        int x_C = inst.classValue();

        for (int u1 = 1; u1 < last; u1++) {
            if (u1 != inst.classIndex()) {
                int x_u1 = inst.value(u1);

                for (int u2 = 0; u2 < u1; u2++) {
                    if (u2 != inst.classIndex()) {
                        int x_u2 = inst.value(u2);

                        int pos1 = u1*x_u1 + u2;
                        int pos2 = x_u2*nc + x_C; 
                        counts_[u1][pos1][pos2]++;
                    }
                }
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
        xyDist_.updateDim(inst, first, last, keyIndex);

        int x_key = inst.classValue();

        for (int u1 = first; u1 < last; u1++) {
            if (u1-first != inst.classIndex()) {
                int x_u1 = inst.value(u1-first);

                for (int u2 = 0; u2 < u1; u2++) {
                    if (u2 == 0 | counts_[u2] != null) {
                        int pos1 = u1*x_u1 + u2;
                        if (u2 < keyIndex) {
                            // u2 is a fact table attribute processed before the dimension key
                            int keyPos1 = keyIndex*x_key + u2;
                            for (int pos2=0; pos2 < counts_[u1][pos1].length; pos2++) {
                                // add the xxy_counts for the key and u2 to the counts for the u1 attribute
                                counts_[u1][pos1][pos2] += counts_[keyIndex][keyPos1][pos2];
                            }
                        } else if (u2 == keyIndex) {
                            // u2 is the dimension key
                            for (int c = 0; c < nc; c++) {
                                // add the xy_counts for the key to the counts for u1 and key
                                int pos2 = x_key*nc + c;
                                counts_[u1][pos1][pos2] += xyDist_.getCount(keyIndex, x_key, c);
                            }
                        } else if (u2 > keyIndex & u2 < first) {
                            // u2 is an attribute from the fact table or another dimension
                            // processed after the dimension key
                            int x_u2 = 0;
                            int n_u2 = paramsPerAtt[u2];
                            for (int k_u2=0; k_u2 < n_u2; k_u2++) {
                                int keyPos1 = u2*k_u2 + keyIndex;
                                for (int c = 0; c < nc; c++) {
                                    // add the xxy_counts for u2 and the key to the counts for u1 and u2
                                    int pos2 = x_u2*nc + c;
                                    int keyPos2 = x_key*nc + c;
                                    counts_[u1][pos1][pos2] += counts_[u2][keyPos1][keyPos2];
                                }
                                x_u2++;
                            }
                        } else if (u2 != first + inst.classIndex()) {
                            // u2 is another attribute in this dimension
                            int x_u2 = inst.value(u2-first);
                            for (int c = 0; c < nc; c++) {
                                // add the xy_counts for the key to the counts for u1 and u2
                                int pos2 = x_u2*nc + c; 
                                counts_[u1][pos1][pos2] += xyDist_.getCount(keyIndex, x_key, c);
                            }
                        }
                    }
                }
            }
        }
    }

    // count for instances x1=v1, x2=v2
    public int getCount(int x1, int v1, int x2, int v2) {
            int c = 0;

            for (int y = 0; y < nc; y++) {
                    c += ref(x1,v1,x2,v2,y);
            }
            return c;
    }

    // p(x1=v1, x2=v2, Y=y) unsmoothed
    public double getCount(int x1, int v1, int x2, int v2, int y) {
            return ref(x1,v1,x2,v2,y);
    }

    // count_[X1=x1][X2=x2][Y=y]
    private double ref(int x1, int v1, int x2, int v2, int y) {
            if (x2 > x1) {
                    int t = x1;
                    x1 = x2;
                    x2 = t;
                    t = v1;
                    v1 = v2;
                    v2 = t;
            }

            //return &count_[x1][v1*x1+x2][v2*noOfClasses_+y];
            int pos1 = v1*x1 + x2;
            int pos2 = v2*nc + y;
            return counts_[x1][pos1][pos2];
    }

    public int getNoAtts() { return n; }

    public int getNoCatAtts() { return n; }

    public int getNoValues(int a) { return paramsPerAtt[a]; }

    public int getNoData() { return N; }

    public void setNoData() { N++; }

    public int getNoClasses() { return nc; }

    public int[] getNoValues() { return paramsPerAtt; }

    @Override
    public String toString() {
        String result = "";
        for (int i=0; i < counts_.length; i++) {
            result = result + "Attribute: " + i + " counts: ";
            if (counts_[i] != null) {
                for (int j=0; j < counts_[i].length; j++) {
                    result = result + "\n   " + j + ": ";
                    if (counts_[i][j] != null) {
                        for (int k=0; k < counts_[i][j].length; k++) {
                            result = result + counts_[i][j][k] + ", ";
                        }
                    }
                }
            }
            result = result + "\n";
        }
        return result;
    }
}
