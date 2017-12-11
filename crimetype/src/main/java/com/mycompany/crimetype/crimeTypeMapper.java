/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimetype;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author dongqi
 */
public class crimeTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        
        if(value.toString().equals("")) return;
        String[] row = value.toString().split(",");
        if(row.length > 11){
            if(row[7].equals("Crime Code")) return;
            String crimeCode = row[7];
            if(row[11].equals("F")) context.write(new Text(crimeCode), one);
        }
        
    }
}
