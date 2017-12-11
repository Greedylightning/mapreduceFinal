/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.javastorm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author dongqiguo
 */
public class EmotionAnalysis extends BaseRichBolt{
    
    private BasicOutputCollector collector;
    Map<String, Integer> words = new HashMap<String, Integer>();
    static int emotionValue = 0;
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        try{
            File file = new File("~/Downloads/emotion");
            InputStreamReader readfile = new InputStreamReader(new FileInputStream(file));
            BufferedReader bfReader = new BufferedReader(readfile);
            String txtline = null;
            while((txtline = bfReader.readLine()) != null){
                String[] arr = txtline.split(",");
                words.put(arr[0], Integer.parseInt(arr[1]));
            }
            bfReader.close();
        }
        catch(FileNotFoundException e){
            ;
        }
        catch(IOException e){
            ;
        }
    }
    
    
    @Override  
    public void declareOutputFields(OutputFieldsDeclarer declarer){   
        declarer.declare(new Fields("emotionValue"));  
    }  

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        if(words.containsKey(word)) emotionValue += words.get(word);
    }
    
    @Override
    public void cleanup(){
        collector.emit(new Values(emotionValue));
    }
}
