package com.asdaraujo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

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

public class KMeansProcedures {
  public static final double[][] points = { {1, 1}, {2, 1}, {1, 2},
                                           {2, 2}, {3, 3}, {8, 8},
                                           {9, 8}, {8, 9}, {9, 9}};
  
  public static void writePointsToFile(List<Vector> points,
                                       String fileName,
                                       FileSystem fs,
                                       Configuration conf) throws IOException {
    Path path = new Path(fileName);
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
        path, LongWritable.class, VectorWritable.class);
    long recNum = 0;
    VectorWritable vec = new VectorWritable();
    for (Vector point : points) {
      vec.set(point);
      writer.append(new LongWritable(recNum++), vec);
    }
    writer.close();
  }
  
  public static List<Vector> getPoints(double[][] raw) {
    List<Vector> points = new ArrayList<Vector>();
    for (int i = 0; i < raw.length; i++) {
      double[] fr = raw[i];
      Vector vec = new RandomAccessSparseVector(fr.length);
      vec.assign(fr);
      points.add(vec);
    }
    return points;
  }
  
  public static List<Vector> readPoints(String inputFile, Configuration conf) throws IOException {
    List<Vector> points = new ArrayList<Vector>();
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(inputFile);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputPath, conf);
    
    BytesWritable key = new BytesWritable();
    Text value = new Text();
    Hashtable<String, Integer> keys = new Hashtable<String, Integer>();
    int idx = 3;
    while (reader.next(key, value)) {
      String[] fields = value.toString().split(Character.toString((char)0x01));
      String provider_id = fields[2];
      String cnt = fields[7];
      String claim_total = fields[8];
      String paid_total = fields[9];

      String code = fields[0]+fields[1];
      if (!keys.containsKey(code))
        keys.put(code, idx++);
      String provider_location = "l"+fields[3]+","+fields[4];
      if (!keys.containsKey(provider_location))
        keys.put(provider_location, ++idx);
      String provider_zipcode = "z"+fields[5];
      if (!keys.containsKey(provider_zipcode))
        keys.put(provider_zipcode, ++idx);
      String referral_region = "r"+fields[6];
      if (!keys.containsKey(referral_region))
        keys.put(referral_region, ++idx);
    }
    reader.close();
    int totalLength = idx;
    reader = new SequenceFile.Reader(fs, inputPath, conf);
    Hashtable<String, Vector> vecs = new Hashtable<String, Vector>();
    while (reader.next(key, value)) {
      String[] fields = value.toString().split(Character.toString((char)0x01));
      String provider_id = fields[2];
      String cnt = fields[7];
      String claim_total = fields[8];
      String paid_total = fields[9];

      String code = fields[0]+fields[1];
      String provider_location = "l"+fields[3]+","+fields[4];
      String provider_zipcode = "z"+fields[5];
      String referral_region = "r"+fields[6];

      Vector vec;
      if (vecs.containsKey(provider_id)) {
        vec = vecs.get(provider_id);
      } else {
        vec = new RandomAccessSparseVector(totalLength);
        vecs.put(provider_id, vec);
        points.add(vec);
        vec.setQuick(0, Double.valueOf(cnt));
        vec.setQuick(1, Double.valueOf(claim_total));
        vec.setQuick(2, Double.valueOf(paid_total));
      }
      vec.setQuick(keys.get(code), 1.0);
      vec.setQuick(keys.get(provider_location), 1.0);
      vec.setQuick(keys.get(provider_zipcode), 1.0);
      vec.setQuick(keys.get(referral_region), 1.0);
    }
    reader.close();
    return points;
  }
  
  public static void main(String args[]) throws Exception {
    
    String inputFile = args[0];
    int k = Integer.valueOf(args[1]);
    System.out.println("Reading points from: " + inputFile);
    System.out.println("K = " + String.valueOf(k));
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    List<Vector> vectors = readPoints(inputFile, conf);
    List<Vector> vectors2 = getPoints(points);
    
    File testData = new File("testdata");
    if (!testData.exists()) {
      testData.mkdir();
    }
    testData = new File("testdata/points");
    if (!testData.exists()) {
      testData.mkdir();
    }
    
    writePointsToFile(vectors, "testdata/points/file1", fs, conf);
    
    Path path = new Path("testdata/clusters/part-00000");
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
        path, Text.class, Kluster.class);
    
    for (int i = 0; i < k; i++) {
      Vector vec = vectors.get(i);
      Kluster cluster = new Kluster(vec, i, new CosineDistanceMeasure());
      writer.append(new Text(cluster.getIdentifier()), cluster);
    }
    writer.close();
    
    KMeansDriver.run(conf, new Path("testdata/points"), new Path("testdata/clusters"),
      new Path("output"), 0.001, 10,
      true, 0.0, false);
      //new Path("output"), new CosineDistanceMeasure(), 0.001, 10,
    
    SequenceFile.Reader reader = new SequenceFile.Reader(fs,
        new Path("output/" + Kluster.CLUSTERED_POINTS_DIR
                 + "/part-m-00000"), conf);

    IntWritable key = new IntWritable();
    WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
    Text dist = new Text("distance");
    while (reader.next(key, value)) {
      System.out.println(String.format("%.15f", Double.valueOf(value.getProperties().get(dist).toString())) + " " + value.toString() + " belongs to cluster "
                         + key.toString());
    }
    reader.close();
  }
  
}