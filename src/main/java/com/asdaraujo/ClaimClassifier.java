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
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

public class ClaimClassifier {
    private static final int FEATURES = 10000;
    private static Multiset<String> overallCounts;

    private static String getAsString(BytesRefWritable ref) {
        int start = ref.getStart();
        int length = ref.getLength();
        try {
            return new String(ref.getData(), start, length, Charsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    public static List<Vector> readPoints(String inputFile, Configuration conf) throws IOException {
        List<Vector> points = new ArrayList<Vector>();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inputFile);
        RCFile.Reader reader = new RCFile.Reader(fs, inputPath, conf);

        LongWritable rows = new LongWritable();
        BytesWritable key = new BytesWritable();
        BytesRefArrayWritable row = new BytesRefArrayWritable();
        Text value = new Text();
        Hashtable<String, Integer> keys = new Hashtable<String, Integer>();
        int idx = 3;
        while (reader.next(rows)) {
            reader.getCurrentRow(row);
            String id = getAsString(row.get(0));
            String review = getAsString(row.get(1));
            String age = getAsString(row.get(2));
            String gender = getAsString(row.get(3));
            String income = getAsString(row.get(4));
            String typeI = getAsString(row.get(5));
            String typeO = getAsString(row.get(6));
            String claims = getAsString(row.get(7));
            System.out.println(String.format("%s:%s:%s:%s:%s:%s:%s:%s", id, review, age, gender, income, typeI, typeO, claims));
//
////            String code = fields[0]+fields[1];
////            if (!keys.containsKey(code))
////                keys.put(code, idx++);
////            String provider_location = "l"+fields[3]+","+fields[4];
////            if (!keys.containsKey(provider_location))
////                keys.put(provider_location, ++idx);
////            String provider_zipcode = "z"+fields[5];
////            if (!keys.containsKey(provider_zipcode))
////                keys.put(provider_zipcode, ++idx);
////            String referral_region = "r"+fields[6];
////            if (!keys.containsKey(referral_region))
////                keys.put(referral_region, ++idx);
        }
        reader.close();
//        int totalLength = idx;
//        reader = new RCFile.Reader(fs, inputPath, conf);
//        Hashtable<String, Vector> vecs = new Hashtable<String, Vector>();
//        while (reader.next(key, value)) {
//            String[] fields = value.toString().split(Character.toString((char)0x01));
//            String provider_id = fields[2];
//            String cnt = fields[7];
//            String claim_total = fields[8];
//            String paid_total = fields[9];
//
//            String code = fields[0]+fields[1];
//            String provider_location = "l"+fields[3]+","+fields[4];
//            String provider_zipcode = "z"+fields[5];
//            String referral_region = "r"+fields[6];
//
//            Vector vec;
//            if (vecs.containsKey(provider_id)) {
//                vec = vecs.get(provider_id);
//            } else {
//                vec = new RandomAccessSparseVector(totalLength);
//                vecs.put(provider_id, vec);
//                points.add(vec);
//                vec.setQuick(0, Double.valueOf(cnt));
//                vec.setQuick(1, Double.valueOf(claim_total));
//                vec.setQuick(2, Double.valueOf(paid_total));
//            }
//            vec.setQuick(keys.get(code), 1.0);
//            vec.setQuick(keys.get(provider_location), 1.0);
//            vec.setQuick(keys.get(provider_zipcode), 1.0);
//            vec.setQuick(keys.get(referral_region), 1.0);
//        }
//        reader.close();
//        return points;
        return null;
    }

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        System.out.println("Reading points from: " + inputFile);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        List<Vector> vectors = readPoints(inputFile, conf);
        System.exit(0);

//        File base = new File(args[0]);
//        overallCounts = HashMultiset.create();
//
//        Map<String, Set<Integer>> traceDictionary = new TreeMap<String, Set<Integer>>();
//        FeatureVectorEncoder encoder = new StaticWordValueEncoder("body");
//        encoder.setProbes(2);
//        encoder.setTraceDictionary(traceDictionary);
//        FeatureVectorEncoder bias = new ConstantValueEncoder("Intercept");
//        bias.setTraceDictionary(traceDictionary);
//        FeatureVectorEncoder lines = new ConstantValueEncoder("Lines");
//        lines.setTraceDictionary(traceDictionary);
//        FeatureVectorEncoder logLines = new ConstantValueEncoder("LogLines");
//        logLines.setTraceDictionary(traceDictionary);
//        Dictionary newsGroups = new Dictionary();
//
//        OnlineLogisticRegression learningAlgorithm =
//                new OnlineLogisticRegression(
//                        20, FEATURES, new L1())
//                        .alpha(1).stepOffset(1000)
//                        .decayExponent(0.9)
//                        .lambda(3.0e-5)
//                        .learningRate(20);
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

