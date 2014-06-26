package com.asdaraujo;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.Dictionary;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.apache.mahout.vectorizer.encoders.StaticWordValueEncoder;

public class PatientClaimReader {
    private static final int READ_BUFFER = 104857600;

    private int numFeatures;
    private Map<String, Set<Integer>> traceDictionary;
    private FeatureVectorEncoder biasEnc;
    private FeatureVectorEncoder ageEnc;
    private FeatureVectorEncoder genderEnc;
    private FeatureVectorEncoder incomeEnc;
    private FeatureVectorEncoder inpatientEnc;
    private FeatureVectorEncoder outpatientEnc;
    private FeatureVectorEncoder claimEnc;
    private List<Pair<Integer,NamedVector>> vectors;

    public PatientClaimReader(int numFeatures, int probes) {
        this.numFeatures = numFeatures;
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
        this.claimEnc.setProbes(probes);
        this.claimEnc.setTraceDictionary(this.traceDictionary);

        this.vectors = new ArrayList<Pair<Integer,NamedVector>>();
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

    public List<Pair<Integer,NamedVector>> readPoints(String inputDir) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(inputDir), false);
        while(files.hasNext()) {
            Path inputFile = files.next().getPath();
            System.out.println("Reading points from: " + inputFile.toString());
    
            RCFile.Reader reader = new RCFile.Reader(fs, inputFile,
                READ_BUFFER, conf, 0, fs.getFileStatus(inputFile).getLen());
    
            Dictionary keys = new Dictionary();
            LongWritable rows = new LongWritable();
            BytesRefArrayWritable row = new BytesRefArrayWritable();
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
    
                Vector v = new RandomAccessSparseVector(this.numFeatures);
                this.biasEnc.addToVector((String)null, 1, v);
                this.ageEnc.addToVector(age, 1, v);
                this.genderEnc.addToVector(gender, 1, v);
                this.incomeEnc.addToVector(income, 1, v);
                this.inpatientEnc.addToVector((String)null, typeI, v);
                this.outpatientEnc.addToVector((String)null, typeO, v);
                for(int i = 0; i < claims.length; i += 2) {
                    this.claimEnc.addToVector(claims[i], Double.valueOf(claims[i+1]), v);
                }
    
                this.vectors.add(new ImmutablePair(keys.intern(review), new NamedVector(v, id)));
            }
            reader.close();
        }

        return this.vectors;
    }

    public Map<String, Set<Integer>> getTraceDictionary() {
        return this.traceDictionary;
    }

}

