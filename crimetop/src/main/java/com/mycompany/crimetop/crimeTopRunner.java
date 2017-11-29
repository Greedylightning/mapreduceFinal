/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimetop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dongqi
 */
public class crimeTopRunner {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        if(args.length != 3){
            System.err.println("Usage: crimeTop <input path> <output path>");
            System.exit(1);
        }
        
        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]);
        Path seinputPath = intermediatePath;
        Path outputPath = new Path(args[2]);
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(crimeTopRunner.class);
        
        job.setMapperClass(crimeTopMapper.class);
        //job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(crimeTopReducer.class);
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, intermediatePath);
        
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(intermediatePath)) hdfs.delete(intermediatePath, true);
        if(hdfs.exists(outputPath)) hdfs.delete(outputPath, true);
        
        int status = job.waitForCompletion(true) ? 0 : 1;
        
        if(status == 0){
            
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "job2");
            job2.setJarByClass(crimeTopRunner.class);
        
            job2.setMapperClass(crimeTopMapper2.class);
            //job.setCombinerClass(AverageReducer.class);
            //job2.setReducerClass(crimeTopReducer.class);
            //job.setNumReduceTasks(0);
        
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job2, intermediatePath);
            FileOutputFormat.setOutputPath(job2, outputPath);
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
