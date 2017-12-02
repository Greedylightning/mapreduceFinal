/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimesentimentalanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author dongqi
 */
public class SentimentAnalysis extends Configured implements Tool{

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
        
        private URI[] files;
        private HashMap<String, String> map = new HashMap<String, String>();
        
        public void setup(Context context) throws IOException{
            
            String line = "";
            
            files = DistributedCache.getCacheFiles(context.getConfiguration());
            Path path = new Path(files[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            
            while((line = br.readLine()) != null){
                String[] splits = line.split("\t");
                map.put(splits[0], splits[1]);
            }
            br.close();
            in.close();
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            
            String tweetId;
            String text;
            String[] tuple = value.toString().split(",");
            if(tuple[0].equals("tweet_id")) return;
            if(tuple.length != 0 && tuple.length > 10){
                tweetId = tuple[0].trim();
                text = tuple[10].trim();
                String[] splits = text.split(" ");
                int sum = 0;
                for(String word : splits){
                    if(map.containsKey(word)){
                        Integer x = new Integer(map.get(word));
                        sum += x;
                    }
                }
                context.write(new Text(tuple[5]), new Text(String.valueOf(sum)));
            }
        }
        
        
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        
        private int netural = 0;
        private int negative = 0;
        private int positive = 0;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            netural = 0;
            negative = 0;
            positive = 0;
            for(Text t : values){
                Integer v = Integer.parseInt(t.toString());
                if(v > 0) positive++;
                else if(v < 0) negative++;
                else netural++;
            }
            context.write(key, new Text("positive: " + positive + "negative: " + negative + "netural: " + netural));
        }
    }
    
    
    @Override
    public int run(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        if(args.length != 3){
            System.err.println("Usage: Parse <input> <output>");
            System.exit(2);
        }
        
        Path output = new Path(args[1]);
        Job job = new Job(conf, "SentimentAnalysis");
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);
        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
        
        
        FileSystem hdfs = FileSystem.get(conf);
	if (hdfs.exists(output)) hdfs.delete(output, true);
	System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
    
    public static void main(String[] args) throws Exception{
        
        ToolRunner.run(new SentimentAnalysis(), args);
    }
}
