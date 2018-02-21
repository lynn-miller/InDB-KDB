/**
 * Copied from Fupla (https://github.com/nayyarzaidi/fupla) SUtils.java class
 * Unused methods have been removed
 */
package KDB_inDB;

import java.util.Arrays;
import java.util.Comparator;

public class SUtils {

	public static double MEsti(double freq1, double freq2, double numValues) {
		double m_MEsti = 1.0;
		double mEsti = (freq1 + m_MEsti / numValues) / (freq2 + m_MEsti);
		return mEsti;
	}

	public static double MEsti(double freq1, double freq2) {
		double mEsti = freq1 / freq2;
		return mEsti;
	}

	public static void normalizeInLogDomain(double[] logs) {
		double logSum = sumInLogDomain(logs);
		for (int i = 0; i < logs.length; i++)
			logs[i] -= logSum;
	}

	public static double sumInLogDomain(double[] logs) {
		// first find max log value
		double maxLog = logs[0];
		int idxMax = 0;
		for (int i = 1; i < logs.length; i++) {
			if (maxLog < logs[i]) {
				maxLog = logs[i];
				idxMax = i;
			}
		}
		// now calculate sum of exponent of differences
		double sum = 0;
		for (int i = 0; i < logs.length; i++) {
			if (i == idxMax) {
				sum++;
			} else {
				sum += Math.exp(logs[i] - maxLog);
			}
		}
		// and return log of sum
		return maxLog + Math.log(sum);
	}

	public static void exp(double[] logs) {
		for (int c = 0; c < logs.length; c++) {
			logs[c] = Math.exp(logs[c]);
		}
	}
        
        public static class compareMI<Double extends Comparable<Double>> implements Comparator<Integer> {
                Double[] args;
                public compareMI(Double[] args) { this.args = args; }
                public int compare( Integer in1, Integer in2 ) {
                    return args[in1.intValue()].compareTo(args[in2.intValue()]);
                }
        }
    
        public static int[] sort(double[] mi) {	
		int n = mi.length;
                Integer[] sortedPositions = new Integer[n];
                Double[] mi_temp = new Double[n];
		for (int i = 0; i < n; i++) {
			sortedPositions[i] = i;
                        mi_temp[i] = new Double(mi[i]);
		}
                compareMI mi_comp = new compareMI(mi_temp);
                Arrays.sort(sortedPositions, mi_comp);
		int[] order = new int[n];
		for (int i = 0; i < n; i++) {
			order[i] = sortedPositions[(n-1) - i];
		}
		return order;
	}
}
