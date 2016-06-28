package com.funcoming.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by LiuFangGuo on 6/28/16.
 */
public class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final String patternString = "\"eventId\":\"(.*?)\"";
    private final Pattern patterner = Pattern.compile(this.patternString);
    private Text group = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = this.patterner.matcher(value.toString());
        String groupKey = "NOMatch";
        if (matcher.find()) {
            groupKey = matcher.group(1);
        }
        this.group.set(groupKey);
        context.write(this.group, value);
    }
}
