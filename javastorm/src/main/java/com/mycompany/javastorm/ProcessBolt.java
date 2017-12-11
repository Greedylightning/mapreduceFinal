/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.javastorm;

import java.util.Map;
import java.util.StringTokenizer;
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
public class ProcessBolt extends BaseRichBolt{

    private BasicOutputCollector collector;
    public ProcessBolt(){}
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("word"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        
    }

    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        StringTokenizer iter = new StringTokenizer(line);
        while(iter.hasMoreElements()){
            collector.emit(new Values(iter.nextToken()));
        }
    }
    
}
