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
import org.apache.hadoop.io.LongWritable;
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
    private static final int LABELS = 2;

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
  
    public void writeClustersToFile(int k, List<Pair<Integer,NamedVector>> vectors,
                                    String fileName) throws IOException {
        Path path = new Path(fileName);
        SequenceFile.Writer writer = new SequenceFile.Writer(this.fs, this.conf,
            path, Text.class, Kluster.class);
        
        Collections.shuffle(vectors);
        for (int i = 0; i < k; i++) {
            Vector vec = vectors.get(i).getValue();
            Kluster cluster = new Kluster(vec, i, new CosineDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();
    }

    private void processResults(int k, String mahoutDir, String outputDir) throws IOException {
        Path clusteredPointsDir = new Path(mahoutDir + "/" + Kluster.CLUSTERED_POINTS_DIR);

        // count occurrences for cluster:label
        int[][] counts = new int[k][LABELS];
        for(int c = 0; c <= 1; c++)
            for(int l = 0; l <= 1; l++)
                counts[c][l] = 0;

        // creates one writer for each class (cluster + label)
        SequenceFile.Writer[][] writers = new SequenceFile.Writer[k][LABELS];
        for(int c = 0; c <= 1; c++)
            for(int l = 0; l <= 1; l++) {
                Path path = new Path(String.format("%s/cluster=%d/label=%d/part-00000", outputDir, c, l));
                writers[c][l] = new SequenceFile.Writer(this.fs, this.conf,
                    path, LongWritable.class, Text.class);
            }

        // read result file
        RemoteIterator<LocatedFileStatus> files = this.fs.listFiles(clusteredPointsDir, false);
        //System.out.println("id,distance,weight,label,cluster");
        int i = 0;
        LongWritable idx = new LongWritable();
        Text name = new Text();
        while(files.hasNext()) {
            Path file = files.next().getPath();
            if (file.getName().charAt(0) != '_') {
                SequenceFile.Reader reader = new SequenceFile.Reader(this.fs, file, this.conf);
                IntWritable key = new IntWritable();
                WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
                Text dist = new Text("distance");
                while (reader.next(key, value)) {
                    NamedVector vec = (NamedVector) value.getVector();
                    double d = Double.valueOf(value.getProperties().get(dist).toString());
                    int label = this.claimLabels.get(vec.getName());
                    int cluster = key.get();
                    //System.out.println(String.format("%s,%.15f,%.2f,%d,%d", 
                    //    vec.getName(),
                    //    d,
                    //    value.getWeight(),
                    //    label,
                    //    cluster));
                    counts[cluster][label]++;

                    // write id to correct file
                    if (outputDir != null) {
                        idx.set(++i);
                        name.set(vec.getName());
                        writers[cluster][label].append(idx, name);
                    }
                }
                reader.close();
            }
        }

        // display information
        showStats(k, counts);

        // close writers
        System.out.println("");
        for(int c = 0; c <= 1; c++)
            for(int l = 0; l <= 1; l++) {
                writers[c][l].close();
                System.out.println(String.format("Points for cluster %d and label %d stored in: %s/cluster=%d/label=%d/part-00000", c, l, outputDir, c, l));
            }
    }

    private void showStats(int k, int[][] counts) {
        int[] clusterCnt = new int[k];
        int[] labelCnt = new int[LABELS];
        // compute total per cluster and label
        for(int c = 0; c <= 1; c++) {
            for(int l = 0; l <= 1; l++) {
                if (c == 0 && l == 0) {
                    clusterCnt[c] = counts[c][l];
                    labelCnt[l] = counts[c][l];
                } else {
                    clusterCnt[c] += counts[c][l];
                    labelCnt[l] += counts[c][l];
                }
            }
        }
        // print stats
        for(int c = 0; c <= 1; c++)
            for(int l = 0; l <= 1; l++)
                System.out.println(String.format("%sCluster %d, Label %d:  %10d points, "
                    + "%7.3f%% of this cluster, %7.3f%% of this label", (l == 0) ? "\n" : "", c, l,
                    counts[c][l], (100.0*counts[c][l])/clusterCnt[c], (100.0*counts[c][l])/labelCnt[l]));
    }

    public void runClustering(int k, int iterations, String inputDir, String workDir, String mahoutDir) throws Exception {

        List<Pair<Integer,NamedVector>> vectors = this.claimReader.readPoints(inputDir);

        String pointsDir = workDir + "/points";
        String clustersDir = workDir + "/clusters";
        String pointsFile = pointsDir + "/points-00001";
        String clustersFile = clustersDir + "/part-00000";

        writeVectorsToFile(vectors, pointsFile);
        writeClustersToFile(k, vectors, clustersFile);

        run(this.conf,
            new Path(pointsDir),
            new Path(clustersDir),
            new Path(mahoutDir),
            0.001,
            iterations,
            true,
            0.0,
            false);
        
    }

    @Override
    public int run(String[] args) throws Exception {
        this.conf = new Configuration();
        this.fs = FileSystem.get(this.conf);

        int k = Integer.valueOf(args[0]);
        int iterations = Integer.valueOf(args[1]);
        String inputDir = args[2];
        String workDir = args[3];
        String mahoutDir = args[4];
        String outputDir = null;
        if (args.length == 6)
            outputDir = args[5];
        System.out.println("K:                 " + String.valueOf(k));
        System.out.println("Iterations:        " + String.valueOf(iterations));
        System.out.println("Input dir:         " + inputDir);
        System.out.println("Work dir:          " + workDir);
        System.out.println("Mahout output dir: " + mahoutDir);
        System.out.println("Final output dir:  " + (outputDir == null ? "<none>" : outputDir));
   
        runClustering(k, iterations, inputDir, workDir, mahoutDir);
        processResults(k, mahoutDir, outputDir);

        return 0;
    }

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new KMeansClaims(), args);
        System.exit(exitCode);
    }
}
