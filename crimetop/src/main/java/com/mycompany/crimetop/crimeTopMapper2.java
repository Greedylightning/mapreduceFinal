/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimetop;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author dongqi
 */
public class crimeTopMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        String[] row = value.toString().split("\t");
        String location = row[0];
        String count = row[1];
        map.put(Integer.parseInt(count), new Text(location));
        
        if(map.size() > 5){
            map.remove(map.firstKey());
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException{
        for(Entry t : map.entrySet()){
            context.write(new Text(t.getValue().toString()), new IntWritable(Integer.parseInt(t.getKey().toString())));
        }
    }
}
