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
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

import org.apache.mahout.text.wikipedia.XmlInputFormat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;
import com.google.common.collect.ImmutableMap;

/**
 * A word count example for Apache Crunch, based on Crunch's example projects.
 */
public class AsciiDelimiterReader extends Configured implements Tool {
    public static final String FIELD_DELIMITER = Character.toString((char)31);

    private static Map<String, Integer> fieldMap = ImmutableMap.of("id", 1, "age", 2, "gndr", 3, "inc", 4);
    private static Pattern p = Pattern.compile("<field\\s+name\\s*=\\s*\"([^\"]+)\"\\s*>([^<]*)</field>");
    private static Text id = new Text("");
    private static Text age = new Text("");
    private static Text gender = new Text("");
    private static Text income = new Text("");

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AsciiDelimiterReader(), args);
    }

    public static PCollection<TupleN> splitLines(PCollection<String> lines) {
        return lines.parallelDo(new DoFn<String, TupleN>() {
            @Override
            public void process(String line, Emitter<TupleN> emitter) {
                emitter.emit(new TupleN(line.split(FIELD_DELIMITER)));
            }
        }, Writables.tuples(Writables.strings()));
    }

    public static PCollection<TupleN> parseXml(PCollection<String> lines) {
        return lines.parallelDo(new MapFn<String, TupleN>() {

            @Override
            public TupleN map(String line) {
                line = line.replace("\n", "");
                Matcher m = p.matcher(line);
                id.set(""); age.set(""); gender.set(""); income.set(""); 
                while (m.find())
                    switch (fieldMap.get(m.group(1))) {
                        case 1: id.set(m.group(2)); break;
                        case 2: age.set(m.group(2)); break;
                        case 3: gender.set(m.group(2)); break;
                        case 4: income.set(m.group(2)); break;
                    }

                return new TupleN(id, age, gender, income);
            }
        }, Writables.tuples(Writables.strings()));
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
        Pipeline pipeline = new MRPipeline(AsciiDelimiterReader.class, getConf());

        // Reference a given text file as a collection of Strings.
        TableSource<Long, String> asciiSource = From.formattedFile(inputPathAscii, AsciiDelimitedFileInputFormat.class,
              Writables.longs(), Writables.strings());
        PTable<Long, String> lines = pipeline.read(asciiSource);

        conf.set("xmlinput.start", "<rows>");
        conf.set("xmlinput.end", "</rows>");   
        TableSource<Long, String> xmlSource = From.formattedFile(inputPathXml, XmlInputFormat.class,
              Writables.longs(), Writables.strings());
        PTable<Long, String> patients = pipeline.read(xmlSource);

        // Define a function that splits each line in a PCollection of Strings into
        // a PCollection made up of the individual words in the file.
        // The second argument sets the serialization format.
        //PCollection<String> words = lines.values().parallelDo(new LineCounter(), Writables.strings());

        // Take the collection of words and remove known stop words.
        //PCollection<String> noStopWords = words.filter(new StopWordFilter());

        // The count method applies a series of Crunch primitives and returns
        // a map of the unique words in the input PCollection to their counts.
        //PTable<String, Long> counts = noStopWords.count();

        // Instruct the pipeline to write the resulting counts to a text file.
        //pipeline.writeTextFile(counts, outputPath);
        PCollection<String> values = lines.values();
        PCollection<TupleN> values2 = splitLines(values);
        pipeline.writeTextFile(values2, outputPath);
        pipeline.writeTextFile(parseXml(patients.values()), outputPath2);

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
