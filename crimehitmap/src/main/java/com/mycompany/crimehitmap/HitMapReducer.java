/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimehitmap;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author dongqi
 */
public class HitMapReducer extends Reducer<Location, NullWritable, Location, NullWritable>{
    
    public void reduce(Location key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        
        int count = 0;
        for(IntWritable i : values){
            count++;
        }
        
        key.setCount(count);
        context.write(key, NullWritable.get());
    }
}
