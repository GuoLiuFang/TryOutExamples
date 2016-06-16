package com.funcoming.hadoop;

import com.funcoming.mapper.MeanMapper;
import com.funcoming.reducer.LongSumReducer;
import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/**
 * Created by LiuFangGuo on 6/15/16.
 */
public class WordMean extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(WordMean.class);

    public void readAndCalcMean(Path outputPath, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        Path mapReduceResultFile = new Path(outputPath, "part-r-00000");
        if (!fileSystem.exists(mapReduceResultFile)) {
            throw new IOException("找不到map reduce的结果,Output not found!");
        }
        String line;
        long count = 0;
        long length = 0;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(mapReduceResultFile), Charsets.UTF_8));
        while ((line = bufferedReader.readLine()) != null) {
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            String type = stringTokenizer.nextToken();
            if (MeanMapper.COUNT.toString().equals(type)) {
                String countStr = stringTokenizer.nextToken();
                count = Long.parseLong(countStr);
            } else if (MeanMapper.LENGTH.toString().equals(type)) {
                String lengthStr = stringTokenizer.nextToken();
                length = Long.parseLong(lengthStr);
            }
        }
        double wordMean = ((double) length) / ((double) count);
        this.LOG.error("最终算出所有字符长度的平均值为" + wordMean);
        bufferedReader.close();
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: word_mean <in> <out>这玩意儿是这么用的");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(2);
        }
        Path outputPath = new Path(remainingArgs[remainingArgs.length - 1]);
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(outputPath, true);
        Job job = new Job(conf, "FunComing word mean");
        job.setJarByClass(WordMean.class);
        job.setMapperClass(MeanMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        for (int i = 0; i < remainingArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        int runningResult = job.waitForCompletion(true) ? 0 : 1;


        return runningResult;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordMean(), args);
    }
}
