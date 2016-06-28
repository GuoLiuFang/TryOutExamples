package com.funcoming.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuFangGuo on 6/28/16.
 */
public class SedAwkMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text line = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String missingFields = context.getConfiguration().get("missingFields");
        this.line.set(value.toString() + missingFields);
        context.write(this.line, NullWritable.get());
    }
}
