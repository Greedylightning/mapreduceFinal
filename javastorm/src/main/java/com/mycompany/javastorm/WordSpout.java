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
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 *
 * @author dongqiguo
 */
public class WordSpout extends BaseRichSpout{
    
    private SpoutOutputCollector collector;
    //private static String[] words = {"星期一","星期二","星期三","星期四","星期五","星期六","星期日"};
    
    public WordSpout(){}
    
    public void nextTuple() {
        //String word = words[new Random().nextInt(words.length)];
        try{
            File file = new File("~/Downloads/oldmanandsea.txt");
            InputStreamReader readfile = new InputStreamReader(new FileInputStream(file));
            BufferedReader bfReader = new BufferedReader(readfile);
            String txtline = null;
            while((txtline = bfReader.readLine()) != null){
                collector.emit(new Values(txtline));
            }
            bfReader.close();
        }
        catch(FileNotFoundException e){
            System.out.println("file not found");
        } catch (IOException ex) {
            System.out.println("ioexception");
        }
        
        
        
    }
    
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2){
        this.collector = arg2;
    }
    
    public void declareOutputFields(OutputFieldsDeclarer arg0){
        arg0.declare(new Fields("word"));
    }
}
