/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimetop;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author dongqi
 */
public class crimeTopMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    //Map<Integer, Text> map = new TreeMap<Integer, Text>();
    private IntWritable one = new IntWritable(1);
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        String[] row = value.toString().split(",");
        String location = row[5];
        context.write(new Text(location), one);
    }
}
