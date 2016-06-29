package com.funcoming.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by LiuFangGuo on 6/28/16.
 */
public class SedAwkMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private final String patternString = "2015-0[1-5]";
    private final Pattern patterner = Pattern.compile(this.patternString);
    private Text line = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("[|]");
//        String[] split = value.toString().split("\\|");
        Matcher matcher = this.patterner.matcher(split[split.length - 1]);
        if (matcher.find()) {
            String missingFields = context.getConfiguration().get("missingFields");
            this.line.set(value.toString() + missingFields);
            context.write(this.line, NullWritable.get());
        }
    }
}
