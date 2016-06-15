package com.funcoming.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by LiuFangGuo on 5/28/16.
 */
public class MultipleOutputsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private MultipleOutputs multipleOutputs;
    private LongWritable result = new LongWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable count :
                values) {
            sum += count.get();
        }
        this.result.set(sum);
        context.write(key, this.result);
        this.multipleOutputs.write("name111", key, this.result);
        this.multipleOutputs.write("name222", key, this.result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.multipleOutputs.close();
    }
}
