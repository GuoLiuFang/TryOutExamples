package com.funcoming.hadoop;

import com.funcoming.mapper.FCMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by LiuFangGuo on 6/2/16.
 */
public class FCSort extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: FCSort <in> <out>这玩意儿是这么用的");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(2);
        }
        Path outputPath = new Path(remainingArgs[remainingArgs.length - 1]);
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(outputPath, true);

        Job job = new Job(conf);
        job.setJobName("全排序");
        job.setJarByClass(FCSort.class);//少了这一行很核心的东西..
        job.setMapperClass(FCMapper.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);//至少要有一个来保证。。最后出来的东西是有顺序的。。。

        for (int i = 0; i < remainingArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new FCSort(), args);
        System.exit(run);
    }
}
