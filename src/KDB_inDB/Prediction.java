package KDB_inDB;

import java.util.HashMap;

/**
 * Class to store predictions for a record
 * Object is created using class probabilities
 * Identifies the predicted class from the class probabilities
 *
 * @author Lynn Miller
 * 
 */
public class Prediction {
    private int rowid;
    private String predictedClass;
    private Double predictedProb;
    private HashMap<String, Double> classProbs;

// Constructor initialises object with first class probability    
    public Prediction(int rowid, String className, double classProb) {
        this.rowid = rowid;
        this.predictedClass = className;
        this.predictedProb = classProb;
        this.classProbs = new HashMap<String, Double>();
        this.classProbs.put(className, classProb);
    }

// Method to add class probabilities    
    public void addClassProb(String className, double classProb) {
        this.classProbs.put(className, classProb);
        if (classProb > predictedProb) {
            this.predictedClass = className;
            this.predictedProb = classProb;
        }
    }
    
    public int getRowid() {return rowid;}
    public String getPredictedClass() {return predictedClass;}
    public Double getPredictedProb() {return predictedProb;}
    public HashMap<String, Double> getClassProbs() {return classProbs;}
    
    @Override
    public String toString() {
        String theString = "Rowid: " + rowid + "; Predicted Class: " + predictedClass + "\nClass probabilities: ";
        for (HashMap.Entry<String, Double> entry : classProbs.entrySet()) {
            theString = theString + String.format("\n  " + entry.getKey() + ": %.4f ", entry.getValue());
        }
        return theString;
    }
}
