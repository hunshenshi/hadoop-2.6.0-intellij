package com.hunhun.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hunhun on 2017/3/6.
 */
public class MiniTest {
    private static MiniMRClientCluster mrCluster;

    private class InternalClass {
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        setup();
        Configuration conf = mrCluster.getConfig();
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String[] otherArgs = new String[2];
        otherArgs[0] = "/Users/netease/workScript/";
        otherArgs[1] = "/Users/netease/szw";
//        String[] otherArgs = args;
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word test");
        job.setJarByClass(MiniTest.class);
        job.setMapperClass(MyMapper.class);
//        job.setNumReduceTasks(0);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        Path output = new Path(otherArgs[otherArgs.length - 1]);
//        conf.set("fs.defaultFS", "hdfs://192.168.244.131:9000");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,
                output);
//        cleanup();
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void setup() throws IOException {
        // create the mini cluster to be used for the tests
        mrCluster = MiniMRClientClusterFactory.create(InternalClass.class, 1,
                new Configuration());
    }

    public static void cleanup() throws IOException {
        // stopping the mini cluster
        mrCluster.stop();
    }

    public static class MyMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for (int i=0; i< words.length; i++) {
                context.write(new Text(words[i]), new IntWritable(1));
            }
        }

    }

    public static class MyReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += 1;
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
