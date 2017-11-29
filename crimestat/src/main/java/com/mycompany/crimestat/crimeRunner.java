/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimestat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author dongqi
 */
public class crimeRunner {
    
    public static void main(String[] args) throws Exception{
        
        Configuration conf = new Configuration();
        
        if(args.length != 2){
            System.err.println("Usage: CrimeBin <input> <output>");
            System.exit(2);
        }
        
        Path input = new Path(args[0]);
	Path outputDir = new Path(args[1]);
        
        Job job = new Job(conf, "Bin");
        
        job.setJarByClass(crimeRunner.class);
        job.setMapperClass(crimeMapper.class);
        //job.setCombinerClass(InvertedIndexReducer.class);
        //job.setPartitionerClass(PartitionPartitioner.class);
        //job.setReducerClass(PartitionReducer.class);
        job.setNumReduceTasks(0);
        //PartitionPartitioner.setMinMonth(job, 1);
        
        
        
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);
        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(outputDir)) hdfs.delete(outputDir, true);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
