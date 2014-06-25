package com.asdaraujo;

import com.google.common.base.Charsets;
import com.google.common.collect.Multiset;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.TokenStream;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;

public class ClaimClassifier {
    private static final int FEATURES = 10000;

    private Map<String, Set<Integer>> traceDictionary;
    private FeatureVectorEncoder biasEnc;
    private FeatureVectorEncoder ageEnc;
    private FeatureVectorEncoder genderEnc;
    private FeatureVectorEncoder incomeEnc;
    private FeatureVectorEncoder inpatientEnc;
    private FeatureVectorEncoder outpatientEnc;
    private FeatureVectorEncoder claimEnc;
    private List<Pair<Integer,Vector>> vectors;
    private OnlineLogisticRegression learningAlgorithm;

    public ClaimClassifier() {
        this.traceDictionary = new TreeMap<String, Set<Integer>>();
        this.biasEnc = new ConstantValueEncoder("bias");
        this.biasEnc.setTraceDictionary(this.traceDictionary);
        this.ageEnc = new StaticWordValueEncoder("age");
        this.ageEnc.setTraceDictionary(this.traceDictionary);
        this.genderEnc = new StaticWordValueEncoder("gender");
        this.genderEnc.setTraceDictionary(this.traceDictionary);
        this.incomeEnc = new StaticWordValueEncoder("income");
        this.incomeEnc.setTraceDictionary(this.traceDictionary);
        this.inpatientEnc = new ConstantValueEncoder("inpatient");
        this.inpatientEnc.setTraceDictionary(this.traceDictionary);
        this.outpatientEnc = new ConstantValueEncoder("outpatient");
        this.outpatientEnc.setTraceDictionary(this.traceDictionary);
        this.claimEnc = new StaticWordValueEncoder("claim");
        this.claimEnc.setProbes(2);
        this.claimEnc.setTraceDictionary(this.traceDictionary);

        this.learningAlgorithm =
            new OnlineLogisticRegression(
                2, FEATURES, new L1())
                .alpha(1).stepOffset(1000)
                .decayExponent(0.9)
                .lambda(3.0e-5)
                .learningRate(20);

        this.vectors = new ArrayList<Pair<Integer,Vector>>();
    }

    public void run(String inputFile) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        readPoints(inputFile, conf);
        train();
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

    public void readPoints(String inputFile, Configuration conf) throws IOException {
        System.out.println("Reading points from: " + inputFile);

        List<Vector> points = new ArrayList<Vector>();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inputFile);
        RCFile.Reader reader = new RCFile.Reader(fs, inputPath, 104857600, conf, 0, fs.getFileStatus(inputPath).getLen());

        Dictionary keys = new Dictionary();
        LongWritable rows = new LongWritable();
        BytesRefArrayWritable row = new BytesRefArrayWritable();
        int idx = 3;
        while (reader.next(rows)) {
            if (rows.get() % 1000 == 0)
                System.out.println(String.format("%d records read", rows.get()));
            reader.getCurrentRow(row);
            String id = getAsString(row.get(0));
            String review = getAsString(row.get(1));
            String age = getAsString(row.get(2));
            String gender = getAsString(row.get(3));
            String income = getAsString(row.get(4));
            double typeI = getAsDouble(row.get(5));
            double typeO = getAsDouble(row.get(6));
            String[] claims = getAsString(row.get(7)).split(",");

            //System.out.println(String.format("%s:%s:%s:%s:%s:%f:%f:%s", id, review, age, gender, income, typeI, typeO, claims.toString()));

            Vector v = new RandomAccessSparseVector(FEATURES);
            biasEnc.addToVector((String)null, 1, v);
            ageEnc.addToVector(age, 1, v);
            genderEnc.addToVector(gender, 1, v);
            incomeEnc.addToVector(income, 1, v);
            inpatientEnc.addToVector((String)null, typeI, v);
            outpatientEnc.addToVector((String)null, typeO, v);
            for(int i = 0; i < claims.length; i += 2) {
                claimEnc.addToVector(claims[i], Double.valueOf(claims[i+1]), v);
            }

            this.vectors.add(new ImmutablePair(keys.intern(review),v));
        }
        reader.close();
    }

    public void train() {
        Collections.shuffle(this.vectors);
        System.out.printf("%d training samples\n", this.vectors.size());

        double averageLL = 0.0;
        double averageCorrect = 0.0;
        double averageLineCount = 0.0;
        int k = 0;
        double step = 0.0;
        int[] bumps = new int[]{1, 2, 5};
        double lineCount = 0;

        for (Pair<Integer,Vector> pair : this.vectors) {
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
        for(String key : traceDictionary.keySet()) {
            Set<Integer> positions = traceDictionary.get(key);
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

    private static void countWords(Analyzer analyzer, Collection<String> words, Reader in) throws IOException {
        TokenStream ts = analyzer.tokenStream("text", in);
        ts.addAttribute(CharTermAttribute.class);
        while (ts.incrementToken()) {
            String s = ts.getAttribute(CharTermAttribute.class).toString();
            words.add(s);
        }
    }
}

