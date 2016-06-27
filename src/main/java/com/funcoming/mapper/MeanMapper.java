package com.funcoming.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by LiuFangGuo on 6/15/16.
 */
public class MeanMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public final static Text COUNT = new Text("count");
    public final static Text LENGTH = new Text("length");
    private final static LongWritable one = new LongWritable(1);

    private LongWritable wordLen = new LongWritable();
//    private String word;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
//        while (stringTokenizer.hasMoreTokens()) {
//            this.word = stringTokenizer.nextToken();
//            this.wordLen.set(this.word.length());
//            context.write(this.COUNT, this.one);
//            context.write(this.LENGTH, this.wordLen);
//        }
        this.wordLen.set(value.toString().length());
        context.write(this.COUNT, this.one);
        context.write(this.LENGTH, this.wordLen);
    }
}
