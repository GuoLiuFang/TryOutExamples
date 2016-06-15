package com.funcoming.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuFangGuo on 6/2/16.
 */
public class FCMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
    private LongWritable sortKey = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String strNum = value.toString();
        this.sortKey.set(Long.parseLong(strNum));
        context.write(this.sortKey, NullWritable.get());
    }
}
