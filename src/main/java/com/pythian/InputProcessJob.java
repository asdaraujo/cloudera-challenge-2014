package com.pythian;

import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.TableSource;
import org.apache.crunch.TupleN;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.apache.hadoop.io.BytesWritable;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;
import com.google.common.collect.ImmutableMap;

/**
 * A word count example for Apache Crunch, based on Crunch's example projects.
 */
public class InputProcessJob extends Configured implements Tool {

    //
    // Private methods for the processing of patient procedure claim records (Ascii-delimited format)
    //

    //private static final String FIELD_DELIMITER = Character.toString((char)31);

    //public static PCollection<TupleN> splitLines(PCollection<String> lines) {
    //    return lines.parallelDo(new MapFn<String, TupleN>() {

    //        @Override
    //        public TupleN map(String line) {
    //            return new TupleN(line.split(FIELD_DELIMITER));
    //        }

    //    }, Writables.tuples(Writables.strings()));
    //}

    private static int debugCounter = 0;

    public static PCollection<PatientClaim> mapArrayToPatientClaim(PCollection<ArrayWritable> lines) {
        return lines.parallelDo(new MapFn<ArrayWritable, PatientClaim>() {

            @Override
            public PatientClaim map(ArrayWritable record) {
                String[] values = record.toStrings();
                return PatientClaim.newBuilder()
                    .setDate(values[0])
                    .setPatientId(values[1])
                    .setClaimId(values[2])
                    .build();
            }

        }, Avros.records(PatientClaim.class));
    }

    //
    // Private methods for the processing of patient profiles (XML format)
    //

    private static final Map<String, Integer> patientRecordFieldMap = ImmutableMap.of("id", 1, "age", 2, "gndr", 3, "inc", 4);
    private static final Pattern patientRecordFieldPattern = Pattern.compile("<field\\s+name\\s*=\\s*\"([^\"]+)\"\\s*>([^<]*)</field>");

    public static PCollection<Patient> parsePatientXml(PCollection<String> lines) {
        return lines.parallelDo(new MapFn<String, Patient>() {

            @Override
            public Patient map(String line) {
                line = line.replace("\n", "");
                Matcher m = patientRecordFieldPattern.matcher(line);
                Patient record = new Patient("", "", "", "");
                while (m.find())
                    switch (patientRecordFieldMap.get(m.group(1))) {
                        case 1: record.setId(m.group(2)); break;
                        case 2: record.setAge(m.group(2)); break;
                        case 3: record.setGender(m.group(2)); break;
                        case 4: record.setIncome(m.group(2)); break;
                    }

                return record;
            }

        }, Avros.records(Patient.class));
    }
    
    //
    // Main methods
    // 

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new InputProcessJob(), args);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: hadoop jar araujo-ccds2-1.0-SNAPSHOT-job.jar"
                                      + " [generic options] apc-ascii-input pnt-xml-input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String inputPathAscii = args[0];
        String inputPathXml = args[1];
        String outputPath = args[2];
        String outputPath2 = args[3];

        Configuration conf = getConf();

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(InputProcessJob.class, getConf());

        // Process patient procedure claim records (Ascii-delimited files)
        TableSource<Long, ArrayWritable> asciiSource = From.formattedFile(inputPathAscii, AsciiDelimitedFileInputFormat.class,
              Writables.longs(), Writables.writables(ArrayWritable.class));
        PCollection<PatientClaim> patientClaims = mapArrayToPatientClaim(pipeline.read(asciiSource).values());
        patientClaims.write(To.avroFile(outputPath));

        // Process patient profiles (XML files)
        conf.set("xmlinput.start", "<rows>");
        conf.set("xmlinput.end", "</rows>");   
        TableSource<Long, String> xmlSource = From.formattedFile(inputPathXml, XmlInputFormat.class,
              Writables.longs(), Writables.strings());
        PCollection<Patient> patients = parsePatientXml(pipeline.read(xmlSource).values());
        patients.write(To.avroFile(outputPath2));

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
