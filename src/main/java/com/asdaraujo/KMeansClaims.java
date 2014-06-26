package com.asdaraujo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class KMeansClaims extends KMeansDriver implements Tool {
    private static final int FEATURES = 10000;

    private PatientClaimReader claimReader;
    private FileSystem fs;
    private Configuration conf;
    private Map<String,Integer> claimLabels;
    private int k;

    public KMeansClaims() throws IOException {
        this.claimReader = new PatientClaimReader(FEATURES, 2);
    }

    public void writeVectorsToFile(List<Pair<Integer,NamedVector>> vectors,
                                   String fileName) throws IOException {
        Path path = new Path(fileName);
        SequenceFile.Writer writer = new SequenceFile.Writer(this.fs, this.conf,
            path, Text.class, VectorWritable.class);
        Text key = new Text();
        VectorWritable vec = new VectorWritable();
        this.claimLabels = new HashMap<String,Integer>();
        for (Pair<Integer,NamedVector> kv : vectors) {
            int label = kv.getKey();
            NamedVector nvec = kv.getValue();
            String id = nvec.getName();
            this.claimLabels.put(id, label);
            key.set(id);
            vec.set(nvec);
            writer.append(key, vec);
        }
        writer.close();
    }
  
    public void writeClustersToFile(List<Pair<Integer,NamedVector>> vectors,
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

    private void showResults(String outputDir) throws IOException {
        Path clusteredPointsDir = new Path(outputDir + "/" + Kluster.CLUSTERED_POINTS_DIR);
        System.out.println("Clustered points dir: " + clusteredPointsDir.toString());

        RemoteIterator<LocatedFileStatus> files = this.fs.listFiles(clusteredPointsDir, false);
        System.out.println("id,distance,weight,label,cluster");
        while(files.hasNext()) {
            Path file = files.next().getPath();
            if (file.getName().charAt(0) != '_') {
                SequenceFile.Reader reader = new SequenceFile.Reader(this.fs, file, this.conf);
                IntWritable key = new IntWritable();
                WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
                Text dist = new Text("distance");
                while (reader.next(key, value)) {
                    double d = Double.valueOf(value.getProperties().get(dist).toString());
                    NamedVector vec = (NamedVector) value.getVector();
                    System.out.println(String.format("%s,%.15f,%.2f,%d,%d", 
                        vec.getName(),
                        d,
                        value.getWeight(),
                        this.claimLabels.get(vec.getName()),
                        key.get()));
                }
                reader.close();
            }
        }
    }

    public void runClustering(String inputFile, String workDir, String outputDir) throws Exception {
        List<Pair<Integer,NamedVector>> vectors = this.claimReader.readPoints(inputFile);

        String pointsDir = workDir + "/points";
        String clustersDir = workDir + "/clusters";
        String pointsFile = pointsDir + "/points-00001";
        String clustersFile = clustersDir + "/clusters-00000";

        writeVectorsToFile(vectors, pointsFile);
        writeClustersToFile(vectors, clustersFile);

        run(this.conf,
            new Path(pointsDir),
            new Path(clustersDir),
            new Path(outputDir),
            0.001,
            1, //10,
            true,
            0.0,
            false);
        
    }

    @Override
    public int run(String[] args) throws Exception {
        this.conf = new Configuration();
        this.fs = FileSystem.get(this.conf);

        String inputFile = args[0];
        this.k = Integer.valueOf(args[1]);
        String workDir = args[2];
        String outputDir = args[3];
        System.out.println("Reading points from: " + inputFile);
        System.out.println("Work dir:            " + workDir);
        System.out.println("Output dir:          " + outputDir);
        System.out.println("K:                   " + String.valueOf(k));
   
        runClustering(inputFile, workDir, outputDir);
        showResults(outputDir);

        return 0;
    }

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new KMeansClaims(), args);
        System.exit(exitCode);
    }
}
