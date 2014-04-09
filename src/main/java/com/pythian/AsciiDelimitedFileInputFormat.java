package com.pythian;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AsciiDelimitedFileInputFormat  extends FileInputFormat<LongWritable, Text>{
    private static final byte[] RECORD_DELIMITER = Character.toString((char)30).getBytes();

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
        InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new LineRecordReader(RECORD_DELIMITER);
    }

}
