package com.asdaraujo;

import com.google.common.base.Charsets;

import java.io.FileNotFoundException;
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
    private int probes;
    private Map<String, Set<Integer>> traceDictionary;
    private FeatureVectorEncoder biasEnc;
    private FeatureVectorEncoder ageEnc;
    private FeatureVectorEncoder genderEnc;
    private FeatureVectorEncoder incomeEnc;
    private FeatureVectorEncoder inpatientEnc;
    private FeatureVectorEncoder outpatientEnc;
    private FeatureVectorEncoder claimEnc;
    private RemoteIterator<LocatedFileStatus> files = null;
    private RCFile.Reader reader = null;
    private int recordCount = 0;
    private Configuration conf;
    private FileSystem fs;
    private LongWritable rows;
    private BytesRefArrayWritable row;

    public PatientClaimReader(String inputDir, int numFeatures, int probes) throws IOException, FileNotFoundException {
        this.numFeatures = numFeatures;
        this.probes = probes;
        clear();

        this.conf = new Configuration();
        this.fs = FileSystem.get(this.conf);
        this.files = this.fs.listFiles(new Path(inputDir), false);
        this.rows = new LongWritable();
        this.row = new BytesRefArrayWritable();
    }

    private void clear() {
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
        this.claimEnc.setProbes(this.probes);
        this.claimEnc.setTraceDictionary(this.traceDictionary);
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

    private static int getAsInteger(BytesRefWritable ref) {
        return Integer.valueOf(getAsString(ref));
    }

    private static double getAsDouble(BytesRefWritable ref) {
        return Double.valueOf(getAsString(ref));
    }

    public List<Pair<Integer,NamedVector>> readPoints(int maxRecords) throws IOException {
        List<Pair<Integer,NamedVector>> vectors = new ArrayList<Pair<Integer,NamedVector>>();
        while(this.files.hasNext() || this.reader != null) {
            if (this.reader == null) {
                Path inputFile = this.files.next().getPath();
                System.out.println("\nReading points from: " + inputFile.toString());
    
                this.reader = new RCFile.Reader(this.fs, inputFile,
                    READ_BUFFER, this.conf, 0, fs.getFileStatus(inputFile).getLen());
            }

            while (reader.next(this.rows)) {
                if (this.rows.get() % 1000 == 0)
                    System.out.print(".");
                reader.getCurrentRow(this.row);
                this.recordCount++;
                String id = getAsString(this.row.get(0));
                int review = getAsInteger(this.row.get(1));
                String age = getAsString(this.row.get(2));
                String gender = getAsString(this.row.get(3));
                String income = getAsString(this.row.get(4));
                double typeI = getAsDouble(this.row.get(5));
                double typeO = getAsDouble(this.row.get(6));
                String[] claims = getAsString(this.row.get(7)).split(",");
    
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
    
                vectors.add(new ImmutablePair(review, new NamedVector(v, id)));
                if (vectors.size() >= maxRecords) {
                    System.out.println("");
                    return vectors;
                }
            }
            reader.close();
            reader = null;
            System.out.println("");
        }
        System.out.printf("\n%d records were read successfully from file.\n", this.recordCount);

        return vectors;
    }

    public Map<String, Set<Integer>> getTraceDictionary() {
        return this.traceDictionary;
    }

}

