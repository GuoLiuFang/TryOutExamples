package com.funcoming.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by LiuFangGuo on 5/28/16.
 */
public class MultipleOutputsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private MultipleOutputs multipleOutputs;
    private IntWritable result = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count :
                values) {
            sum += count.get();
        }
        result.set(sum);
        context.write(key, result);
        multipleOutputs.write("name111",key,result);
        multipleOutputs.write("name222",key,result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
