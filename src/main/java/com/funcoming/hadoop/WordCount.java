package com.funcoming.hadoop;

import com.funcoming.mapper.TokenizerMapper;
import com.funcoming.reducer.IntSumReducer;
import com.funcoming.reducer.MultipleOutputsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by LiuFangGuo on 5/28/16.
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>这玩意儿是这么用的");
            System.exit(2);
        }
        Path inputPath = new Path(remainingArgs[0]);
        Path outputPath = new Path(remainingArgs[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(outputPath, true);
        Job job = new Job(configuration, "FunComing word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(MultipleOutputsReducer.class);
        job.setReducerClass(MultipleOutputsReducer.class);

//        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);//prevent create zero-sized default output


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        fileSystem.delete(outputPath, true);
        MultipleOutputs.addNamedOutput(job, "name111", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "name222", TextOutputFormat.class, Text.class, IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
