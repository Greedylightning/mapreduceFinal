/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimehitmap;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author dongqi
 */
public class HitMapMapper extends Mapper<LongWritable, Text, Location, NullWritable>{
    
    private Location pos = new Location();
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        String[] row = value.toString().split(",");
        String lat = row[1].substring(4);
        String lng = row[0].substring(6, row[0].length() - 2);
        int count = Integer.parseInt(row[2].substring(6));
        pos.setLat(lat);
        pos.setLng(lng);
        pos.setCount(count);
        context.write(pos, NullWritable.get());
        
        
        //"(34.0454, -118.3157)"
    }
}
