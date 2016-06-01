package com.funcoming.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by LiuFangGuo on 5/29/16.
 */
public class Sort<K, V> extends Configured implements Tool {
    public static final String REDUCES_PER_HOST = "mapreduce.sort.reducesperhost";
    private Job job = null;

    static int printUsage() {
        System.out.println("sort [-m <maps>] [-r <reduces>] " +
                "[-inFormat <input format class>] " +
                "[-outFormat <output format class>] " +
                "[-outKey <output key class>] " +
                "[-outValue <output value class>] " +
                "[-totalOrder <pcnt> <num samples> <max splits>] " +
                "<input> <output>"
        );
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }


    /*
    * The main driver for sort program.
    * Invoke this method to submit the map/reduce job.
    * */
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        JobClient jobClient = new JobClient(configuration);
        ClusterStatus clusterStatus = jobClient.getClusterStatus();
        int num_reduces = (int) (clusterStatus.getMaxReduceTasks() * 0.9);
        String sort_reduces = configuration.get(REDUCES_PER_HOST);
        if (sort_reduces != null) {
            num_reduces = clusterStatus.getTaskTrackers() * Integer.parseInt(sort_reduces);
        }
//        Class<? extends InputFormat> inputFormatClass = SequenceFileInputFormat.class;
        Class<? extends InputFormat> inputFormatClass = TextInputFormat.class;
        Class<? extends OutputFormat> outputFormatClass = TextOutputFormat.class;
//        Class<? extends OutputFormat> outputFormatClass = SequenceFileOutputFormat.class;
        Class<? extends WritableComparable> outputKeyClass = LongWritable.class;
//        Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
        Class<? extends Writable> outputValueClass = Text.class;
//        Class<? extends Writable> outputValueClass = BytesWritable.class;
        List<String> otherArgs = new ArrayList<String>();
        InputSampler.Sampler<K, V> inputSampler = null;
        for (int i = 0; i < args.length; i++) {
            try {
                if ("-r".equals(args[i])) {
                    num_reduces = Integer.parseInt(args[++i]);
                } else if ("-inFormat".equals(args[i])) {
                    inputFormatClass = Class.forName(args[++i]).asSubclass(InputFormat.class);
                } else if ("-outFormat".equals(args[i])) {
                    outputFormatClass = Class.forName(args[++i]).asSubclass(OutputFormat.class);
                } else if ("-outKey".equals(args[i])) {
                    outputKeyClass = Class.forName(args[++i]).asSubclass(WritableComparable.class);
                } else if ("-outValue".equals(args[i])) {
                    outputValueClass = Class.forName(args[++i]).asSubclass(Writable.class);
                } else if ("-totalOrder".equals(args[i])) {
                    double pcnt = Double.parseDouble(args[++i]);
                    int numSamples = Integer.parseInt(args[++i]);
                    int maxSplits = Integer.parseInt(args[++i]);
                    if (0 >= maxSplits) {
                        maxSplits = Integer.MAX_VALUE;
                    }
                    inputSampler = new InputSampler.RandomSampler<K, V>(pcnt, numSamples, maxSplits);
                } else {
                    otherArgs.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
                return printUsage();
            }
        }

        job = new Job(configuration);
        job.setJobName("FC-Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        job.setNumReduceTasks(num_reduces);

        job.setInputFormatClass(inputFormatClass);
        job.setOutputFormatClass(outputFormatClass);

        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);

        if (otherArgs.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " + otherArgs.size() + " instead of 2.");
            return printUsage();
        }

        FileInputFormat.setInputPaths(job, otherArgs.get(0));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(otherArgs.get(1)), true);

        if (inputSampler != null) {
            System.out.println("Sampling input to effect total-order sort...");
            job.setPartitionerClass(TotalOrderPartitioner.class);
            Path inputDir = FileInputFormat.getInputPaths(job)[0];
            Path partitionFile = new Path(inputDir, "_sortPartitioning_glf");
            TotalOrderPartitioner.setPartitionFile(configuration, partitionFile);
            InputSampler.writePartitionFile(job, inputSampler);
            URI partitionUri = new URI(partitionFile.toString() + "#" + "_sortPartitioning_glf");
            DistributedCache.addCacheFile(partitionUri, configuration);
        }
        System.out.println("Running on " + clusterStatus.getTaskTrackers() + " nodes to sort from " + FileInputFormat.getInputPaths(job)[0] + " into " + FileOutputFormat.getOutputPath(job) + " with " + num_reduces + " reduces. ");
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int ret = job.waitForCompletion(true) ? 0 : 1;
        Date end_time = new Date();
        System.out.println("Job ended: " + end_time);
        System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Sort(), args);
        System.exit(res);
    }
}































