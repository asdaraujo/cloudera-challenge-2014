package com.asdaraujo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class KMeansClaims {
    private static final int FEATURES = 10000;

    private PatientClaimReader claimReader;
    private FileSystem fs;
    private Configuration conf;
    private int k;

    public KMeansClaims(int k) throws IOException {
        this.claimReader = new PatientClaimReader(FEATURES, 2);
        this.conf = new Configuration();
        this.fs = FileSystem.get(conf);
        this.k = k;
    }

    public void writeVectorsToFile(List<Pair<Integer,Vector>> vectors,
                                   String fileName) throws IOException {
        Path path = new Path(fileName);
        SequenceFile.Writer writer = new SequenceFile.Writer(this.fs, this.conf,
            path, LongWritable.class, VectorWritable.class);
        LongWritable key = new LongWritable();
        VectorWritable vec = new VectorWritable();
        for (Pair<Integer,Vector> kv : vectors) {
            key.set(kv.getKey());
            vec.set(kv.getValue());
            writer.append(key, vec);
        }
        writer.close();
    }
  
    public void writeClustersToFile(List<Pair<Integer,Vector>> vectors,
                                    String fileName) throws IOException {
        Path path = new Path(fileName);
        SequenceFile.Writer writer = new SequenceFile.Writer(this.fs, this.conf,
            path, Text.class, Kluster.class);
        
        Collections.shuffle(vectors);
        for (int i = 0; i < this.k; i++) {
            Vector vec = vectors.get(i).getValue();
            Kluster cluster = new Kluster(vec, i, new CosineDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();
    }

//    public static List<Vector> readPoints(String inputFile, Configuration conf) throws IOException {
//        List<Vector> points = new ArrayList<Vector>();
//        FileSystem fs = FileSystem.get(conf);
//        Path inputPath = new Path(inputFile);
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, conf);
//    
//        BytesWritable key = new BytesWritable();
//        Text value = new Text();
//        Hashtable<String, Integer> keys = new Hashtable<String, Integer>();
//        int idx = 3;
//        while (reader.next(key, value)) {
//            String[] fields = value.toString().split(Character.toString((char)0x01));
//            String provider_id = fields[2];
//            String cnt = fields[7];
//            String claim_total = fields[8];
//            String paid_total = fields[9];
//
//            String code = fields[0]+fields[1];
//            if (!keys.containsKey(code))
//                keys.put(code, idx++);
//            String provider_location = "l"+fields[3]+","+fields[4];
//            if (!keys.containsKey(provider_location))
//                keys.put(provider_location, ++idx);
//            String provider_zipcode = "z"+fields[5];
//            if (!keys.containsKey(provider_zipcode))
//                keys.put(provider_zipcode, ++idx);
//            String referral_region = "r"+fields[6];
//            if (!keys.containsKey(referral_region))
//                keys.put(referral_region, ++idx);
//        }
//        reader.close();
//        int totalLength = idx;
//        reader = new SequenceFile.Reader(fs, inputPath, conf);
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
//    }
    public void run(String inputFile, String outputDir) throws Exception {
        List<Pair<Integer,Vector>> vectors = this.claimReader.readPoints(inputFile);

        String pointsDir = outputDir + "/points";
        String clustersDir = outputDir + "/clusters";
        String pointsFile = pointsDir + "/file-00001";
        String clustersFile = clustersDir + "/part-00000";

        writeVectorsToFile(vectors, pointsFile);
        writeClustersToFile(vectors, clustersFile);

        KMeansDriver.run(this.conf,
            new Path(pointsDir),
            new Path(clustersDir),
            new Path("output"),
            0.001,
            10,
            true,
            0.0,
            false);
        
    }

    public static void main(String args[]) throws Exception {
    
        String inputFile = args[0];
        int k = Integer.valueOf(args[1]);
        String outputDir = args[2];
        System.out.println("Reading points from: " + inputFile);
        System.out.println("K:                   " + String.valueOf(k));
        System.out.println("Output dir:          " + outputDir);
   
        KMeansClaims kmc = new KMeansClaims(k);
        kmc.run(inputFile, outputDir);

        System.exit(0);
        
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
//            new Path("output/" + Kluster.CLUSTERED_POINTS_DIR
//                     + "/part-m-00000"), conf);
//    
//        IntWritable key = new IntWritable();
//        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
//        Text dist = new Text("distance");
//        while (reader.next(key, value)) {
//          System.out.println(String.format("%.15f", Double.valueOf(value.getProperties().get(dist).toString())) + " " + value.toString() + " belongs to cluster "
//                             + key.toString());
//        }
//        reader.close();
    }
}
