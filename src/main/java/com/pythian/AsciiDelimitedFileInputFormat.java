package com.pythian;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AsciiDelimitedFileInputFormat  extends FileInputFormat<LongWritable, ArrayWritable>{

    @Override
    public RecordReader<LongWritable, ArrayWritable> createRecordReader(
        InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new AsciiDelimitedLineRecordReader();
    }

}
