package com.asdaraujo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import java.util.Hashtable;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.ContinuousValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

public class ClaimClassifier {
    private static final int FEATURES = 10000;
    private static Multiset<String> overallCounts;

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

    }

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        ClaimClassifier cc = new ClaimClassifier();
        cc.run(inputFile);

        System.exit(0);

//        overallCounts = HashMultiset.create();
//
//        List<File> files = new ArrayList<File>();
//        for (File newsgroup : base.listFiles()) {
//            newsGroups.intern(newsgroup.getName());
//            files.addAll(Arrays.asList(newsgroup.listFiles()));
//        }
//
//        Collections.shuffle(files);
//        System.out.printf("%d training files\n", files.size());
//
//        double averageLL = 0.0;
//        double averageCorrect = 0.0;
//        double averageLineCount = 0.0;
//        int k = 0;
//        double step = 0.0;
//        int[] bumps = new int[]{1, 2, 5};
//        double lineCount = 0;
//
//        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
//
//        Splitter onColon = Splitter.on(":").trimResults();
//
//        for (File file : files) {
//
//            BufferedReader reader = new BufferedReader(new FileReader(file));
//            String ng = file.getParentFile().getName();
//            int actual = newsGroups.intern(ng);
//            Multiset<String> words = ConcurrentHashMultiset.create();
//
//            String line = reader.readLine();
//            while (line != null && line.length() > 0) {
//                if (line.startsWith("Lines:")) {
//                    String count = Iterables.get(onColon.split(line), 1);
//                    try {
//                        lineCount = Integer.parseInt(count);
//                        averageLineCount += (lineCount - averageLineCount)
//                                / Math.min(k + 1, 1000);
//                    } catch (NumberFormatException e) {
//                        lineCount = averageLineCount;
//                    }
//                }
//                boolean countHeader = (
//                        line.startsWith("From:") || line.startsWith("Subject:")||
//                                line.startsWith("Keywords:")|| line.startsWith("Summary:"));
//                do {
//                    StringReader in = new StringReader(line);
//                    if (countHeader) {
//                        countWords(analyzer, words, in);
//                    }
//                    line = reader.readLine();
//                } while (line.startsWith(" "));
//            }
//            countWords(analyzer, words, reader);
//            reader.close();
//
//
//            Vector v = new RandomAccessSparseVector(FEATURES);
//            bias.addToVector((String)null, 1, v);
//            lines.addToVector((String)null, lineCount / 30, v);
//            logLines.addToVector((String)null, Math.log(lineCount + 1), v);
//            for (String word : words.elementSet()) {
//                encoder.addToVector(word, Math.log(1 + words.count(word)), v);
//            }
//
//
//            double mu = Math.min(k + 1, 200);
//            double ll = learningAlgorithm.logLikelihood(actual, v);
//            averageLL = averageLL + (ll - averageLL) / mu;
//
//            Vector p = new DenseVector(20);
//            learningAlgorithm.classifyFull(p, v);
//            int estimated = p.maxValueIndex();
//
//            int correct = (estimated == actual? 1 : 0);
//            averageCorrect = averageCorrect + (correct - averageCorrect) / mu;
//
//            learningAlgorithm.train(actual, v);
//            k++;
//            int bump = bumps[(int) Math.floor(step) % bumps.length];
//            int scale = (int) Math.pow(10, Math.floor(step / bumps.length));
//            if (k % (bump * scale) == 0) {
//                step += 0.25;
//                System.out.printf("%10d %10.3f %10.3f %10.2f %s %s\n",
//                        k, ll, averageLL, averageCorrect * 100, ng,
//                        newsGroups.values().get(estimated));
//            }
//            learningAlgorithm.close();
//        }
//
    }

    private static void countWords(Analyzer analyzer, Collection<String> words, Reader in) throws IOException {
        TokenStream ts = analyzer.tokenStream("text", in);
        ts.addAttribute(CharTermAttribute.class);
        while (ts.incrementToken()) {
            String s = ts.getAttribute(CharTermAttribute.class).toString();
            words.add(s);
        }
            /*overallCounts.addAll(words);*/
    }
}

