/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crimestat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author dongqi
 */
public class crimeMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
    
    private MultipleOutputs<Text, NullWritable> mos = null;
    
    protected void setup(Context context){
        mos = new MultipleOutputs<>(context);
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split(",");
        String sex = row[11];
        int age;
        if(sex.equals("F")){
            if(!row[10].equals("")){
                age = Integer.parseInt(row[10]);
            }
            else{
                return ;
            }
            if(age >= 10 && age <= 20){
                mos.write("bins", value, NullWritable.get(), "F 10-20");
            }
            else if(age > 20 && age <= 30){
                mos.write("bins", value, NullWritable.get(), "F 20-30");
            }
            else if(age > 30 && age <= 40){
                mos.write("bins", value, NullWritable.get(), "F 30-40");
            }
            else if(age > 40 && age <= 50){
                mos.write("bins", value, NullWritable.get(), "F 40-50");
            }
            else if(age > 50 && age <= 60){
                mos.write("bins", value, NullWritable.get(), "F 50-60");
            }
            else if(age > 60 && age <= 70){
                mos.write("bins", value, NullWritable.get(), "F 60-70");
            }
            else if(age > 70 && age <= 80){
                mos.write("bins", value, NullWritable.get(), "F 70-80");
            }
            else{
                mos.write("bins", value, NullWritable.get(), "F >80");
            }
        }
        else if(sex.equals("M")){
            if(!row[10].equals("")){
                age = Integer.parseInt(row[10]);
            }
            else{
                return ;
            }
            if(age >= 10 && age <= 20){
                mos.write("bins", value, NullWritable.get(), "M 10-20");
            }
            else if(age > 20 && age <= 30){
                mos.write("bins", value, NullWritable.get(), "M 20-30");
            }
            else if(age > 30 && age <= 40){
                mos.write("bins", value, NullWritable.get(), "M 30-40");
            }
            else if(age > 40 && age <= 50){
                mos.write("bins", value, NullWritable.get(), "M 40-50");
            }
            else if(age > 50 && age <= 60){
                mos.write("bins", value, NullWritable.get(), "M 50-60");
            }
            else if(age > 60 && age <= 70){
                mos.write("bins", value, NullWritable.get(), "M 60-70");
            }
            else if(age > 70 && age <= 80){
                mos.write("bins", value, NullWritable.get(), "M 70-80");
            }
            else{
                mos.write("bins", value, NullWritable.get(), "M >80");
            }
        }
        else{
            ;
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException{
        mos.close();
    }
}
