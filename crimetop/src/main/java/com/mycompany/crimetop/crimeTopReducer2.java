/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimetop;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author dongqi
 */
public class crimeTopReducer2 extends Reducer<NullWritable, Text, NullWritable, Text>{
    
    private TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();
    
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    }
}
