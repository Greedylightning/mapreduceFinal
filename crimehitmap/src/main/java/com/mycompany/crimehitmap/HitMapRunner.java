/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimehitmap;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dongqi
 */
public class HitMapRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        if(args.length != 2){
            System.err.println("Usage: crimeHitmap <input path> <output path>");
            System.exit(1);
        }
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(HitMapRunner.class);
        
        job.setMapperClass(HitMapMapper.class);
        //job.setCombinerClass(HitMapReducer.class);
        //job.setReducerClass(HitMapReducer.class);
        //job.setNumReduceTasks(1);
        
        job.setMapOutputKeyClass(Location.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        job.setOutputKeyClass(Location.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(outputPath)) hdfs.delete(outputPath, true);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
