package com.asdaraujo;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

public class ClaimClassifier {
    private static final int FEATURES = 10000;

    private OnlineLogisticRegression learningAlgorithm;
    private PatientClaimReader claimReader;

    public ClaimClassifier() {
        this.learningAlgorithm =
            new OnlineLogisticRegression(
                2, FEATURES, new L1())
                .alpha(1).stepOffset(1000)
                .decayExponent(0.9)
                .lambda(3.0e-5)
                .learningRate(20);

        this.claimReader = new PatientClaimReader(FEATURES, 2);
    }

    public void run(String inputFile) throws IOException {
        List<Pair<Integer,Vector>> vectors = this.claimReader.readPoints(inputFile);
        train(vectors);
    }

    private static String getAsString(BytesRefWritable ref) {
        int start = ref.getStart();
        int length = ref.getLength();
        try {
            return new String(ref.getData(), start, length, Charsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    private static double getAsDouble(BytesRefWritable ref) {
        return Double.valueOf(getAsString(ref));
    }

    public void train(List<Pair<Integer,Vector>> vectors) {
        Collections.shuffle(vectors);
        System.out.printf("%d training samples\n", vectors.size());

        double averageLL = 0.0;
        double averageCorrect = 0.0;
        double averageLineCount = 0.0;
        int k = 0;
        double step = 0.0;
        int[] bumps = new int[]{1, 2, 5};
        double lineCount = 0;

        for (Pair<Integer,Vector> pair : vectors) {
            int actual = pair.getKey();
            Vector v = pair.getValue();

            double mu = Math.min(k + 1, 200);
            double ll = learningAlgorithm.logLikelihood(actual, v);
            averageLL = averageLL + (ll - averageLL) / mu;

            Vector p = new DenseVector(2);
            learningAlgorithm.classifyFull(p, v);
            int estimated = p.maxValueIndex();

            int correct = (estimated == actual? 1 : 0);
            averageCorrect = averageCorrect + (correct - averageCorrect) / mu;

            learningAlgorithm.train(actual, v);
            k++;
            int bump = bumps[(int) Math.floor(step) % bumps.length];
            int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
            if (k % (bump * scale) == 0) {
                step += 0.25;
                System.out.printf("%10d %10.3f %10.3f %10.2f %d\n",
                        k, ll, averageLL, averageCorrect * 100, 
                        estimated);
            }
            learningAlgorithm.close();
        }

        Matrix beta = learningAlgorithm.getBeta();
        System.out.println(String.format("Matrix: %d x %d", beta.rowSize(), beta.columnSize()));

        System.out.println("Trace dictionary:");
        for(String key : this.claimReader.getTraceDictionary().keySet()) {
            Set<Integer> positions = this.claimReader.getTraceDictionary().get(key);
            StringBuilder sb = null;
            for(int pos : positions) {
                String coef = String.format("%7.3f", beta.get(0, pos));
                if (sb == null)
                    sb = new StringBuilder(coef);
                else
                    sb.append("," + coef);
            }
            System.out.println(String.format("%-20s: %-20s %s", key, positions, sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        ClaimClassifier cc = new ClaimClassifier();
        cc.run(inputFile);
    }

}

