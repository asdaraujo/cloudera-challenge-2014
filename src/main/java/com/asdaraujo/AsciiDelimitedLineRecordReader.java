package com.asdaraujo;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AsciiDelimitedLineRecordReader extends RecordReader<LongWritable, ArrayWritable> {
    private static final Log LOG = LogFactory.getLog(AsciiDelimitedLineRecordReader.class);

    private static final byte[] RECORD_DELIMITER = Character.toString((char)30).getBytes();
    private static final String FIELD_DELIMITER = Character.toString((char)31);

    private LineRecordReader lineRecordReader;

    private ArrayWritable splitLine(Text line) {
        String[] values = line.toString().split(this.FIELD_DELIMITER);
        ArrayWritable arrayWritable = new ArrayWritable(values);
        return arrayWritable;
    }

    public AsciiDelimitedLineRecordReader() {
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        this.lineRecordReader = new LineRecordReader(this.RECORD_DELIMITER);
        this.lineRecordReader.initialize(genericSplit, context);
    }

    public boolean nextKeyValue() throws IOException {
        return this.lineRecordReader.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() {
        return this.lineRecordReader.getCurrentKey();
    }

    @Override
    public ArrayWritable getCurrentValue() {
        return splitLine(this.lineRecordReader.getCurrentValue());
    }

    public float getProgress() throws IOException {
        return this.lineRecordReader.getProgress();
    }
  
    public synchronized void close() throws IOException {
        this.lineRecordReader.close();
    }
}
