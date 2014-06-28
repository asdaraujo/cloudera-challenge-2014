package com.asdaraujo;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

public class ClaimClassifier {
    private static final int FEATURES = 10000;
    private static final int LABELS = 2;
    private static final char SEQUENCE_FILE_FIELD_SEPARATOR = 1;
    private static final char CR = 13;
    private static final int[] BUMPS = new int[]{1, 2, 5};
    private static final int MAX_STEP = 1000000;

    private OnlineLogisticRegression learningAlgorithm;

    private PatientClaimReader claimReader;
    private SequenceFile.Writer resultsWriter;
    private double averageLL = 0.0;
    private double averageCorrect = 0.0;
    private double step = 0.0;
    private int k = 0;
    private int[] counts = null;

    public ClaimClassifier() {
        this.learningAlgorithm =
            new OnlineLogisticRegression(
                LABELS, FEATURES, new L1())
                .alpha(1).stepOffset(1000)
                .decayExponent(0.9)
                .lambda(3.0e-5)
                .learningRate(20);
    }

    private void prepareDataLoad(String inputFile, boolean verbose) throws IOException {
        prepareDataLoad(inputFile, null, verbose);
    }

    private void prepareDataLoad(String inputFile, String outputFile, boolean verbose) throws IOException {
        if (this.claimReader != null)
            finishDataLoad();
        this.claimReader = new PatientClaimReader(inputFile, FEATURES, LABELS, verbose);
        
        if (outputFile != null) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(outputFile);
            this.resultsWriter = new SequenceFile.Writer(fs, conf,
                path, BytesWritable.class, Text.class);
        }

        averageLL = 0.0;
        averageCorrect = 0.0;
        step = 0.0;
        k = 0;
        counts = new int[LABELS];
        for(int l = 0; l < LABELS; l++)
            this.counts[l] = 0;
    }

    private void finishDataLoad() throws IOException {
        if (this.claimReader != null) {
            this.claimReader.close();
            this.claimReader = null;
        }
        if (this.resultsWriter != null) {
            this.resultsWriter.close();
            this.resultsWriter = null;
        }
    }

    public void run(String trainingFile, String testFile, String resultsFile) throws IOException {
        List<Pair<Integer,NamedVector>> vectors;

        // training phase
        System.out.println("\nTraining the classifier");
        prepareDataLoad(trainingFile, true);
        vectors = this.claimReader.readPoints(200000);
        while (vectors.size() > 0) {
            train(vectors);
            vectors = this.claimReader.readPoints(200000);
        }

        // print beat coeficients
        System.out.println("\nTrace dictionary:");
        Matrix beta = this.learningAlgorithm.getBeta();
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
            System.out.printf("%-20s: %-20s %s\n", key, positions, sb.toString());
        }
        finishDataLoad();

        // testing phase
        System.out.println("\nTesting");
        prepareDataLoad(testFile, resultsFile, false);
        vectors = this.claimReader.readPoints(200000);
        while (vectors.size() > 0) {
            test(vectors, this.resultsWriter);
            vectors = this.claimReader.readPoints(200000);
        }
        finishDataLoad();
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

    public void train(List<Pair<Integer,NamedVector>> vectors) throws IOException {
        processVectors(vectors, true, null);
    }

    public void test(List<Pair<Integer,NamedVector>> vectors,
        SequenceFile.Writer writer) throws IOException {
        processVectors(vectors, false, writer);
    }

    public void processVectors(List<Pair<Integer,NamedVector>> vectors,
        boolean train, SequenceFile.Writer writer) throws IOException {

        BytesWritable recordKey = new BytesWritable("".getBytes());
        Text value = new Text();

        if (train)
            Collections.shuffle(vectors);

        double mu = -1.0;
        double ll = -1.0;
        int actual = -1;
        for (Pair<Integer,NamedVector> pair : vectors) {
            NamedVector v = pair.getValue();
            if (train) {
                actual = pair.getKey();
                mu = Math.min(this.k + 1, 200);
                ll = learningAlgorithm.logLikelihood(actual, v);
                this.averageLL = this.averageLL + (ll - this.averageLL) / mu;
            }

            Vector p = new DenseVector(LABELS);
            learningAlgorithm.classifyFull(p, v);
            int estimated = p.maxValueIndex();
            this.counts[estimated]++;

            if (writer != null) {
                value.set(String.format("%s%c%d%c01%f", v.getName(),
                    SEQUENCE_FILE_FIELD_SEPARATOR, estimated,
                    SEQUENCE_FILE_FIELD_SEPARATOR, p.get(estimated)));
                writer.append(recordKey, value);
            }

            if (train) {
                int correct = (estimated == actual ? 1 : 0);
                this.averageCorrect = this.averageCorrect + (correct - this.averageCorrect) / mu;

                learningAlgorithm.train(actual, v);
                learningAlgorithm.close();
            }
            this.k++;
            int bump = this.BUMPS[(int) Math.floor(this.step) % this.BUMPS.length];
            int scale = (int) Math.pow(10, Math.floor(this.step / this.BUMPS.length));
            if (this.k % Math.min(MAX_STEP, bump * scale) == 0) {
                this.step += 0.25;
                if (train)
                    System.out.printf("%10d %10.3f %10.3f %10.2f %d\n",
                        this.k, ll, this.averageLL, this.averageCorrect * 100, 
                        estimated);
                else
                    System.out.printf("%c%10d, per label: %s\n", CR, this.k, Arrays.toString(this.counts));
            }
        }

    }

    public static void main(String[] args) throws IOException {
        String trainingFile = args[0];
        String testFile = args[1];
        String resultsFile = args[2];
        ClaimClassifier cc = new ClaimClassifier();
        cc.run(trainingFile, testFile, resultsFile);
    }

}

