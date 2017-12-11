/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimetype;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author dongqi
 */
public class crimeTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        
        int count = 0;
        for(IntWritable i : values){
            count++;
        }
        context.write(key, new IntWritable(count));
    }
}
