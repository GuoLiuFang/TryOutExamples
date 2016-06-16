package com.funcoming.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LiuFangGuo on 5/28/16.
 */
public class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable count :
                values) {
            sum += count.get();
        }
        this.result.set(sum);
        context.write(key, this.result);
    }
}
