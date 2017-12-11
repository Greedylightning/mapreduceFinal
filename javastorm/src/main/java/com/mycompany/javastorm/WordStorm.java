/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.javastorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 *
 * @author dongqiguo
 */
public class WordStorm {
    
    public void WordStorm(){}
    
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WordSpout(), 5);
        builder.setBolt("split", new ProcessBolt(), 5).shuffleGrouping("spout");
        builder.setBolt("count", new EmotionAnalysis(), 5).fieldsGrouping("split", new Fields("word"));
        
        
        final Config config = new Config();
        config.setDebug(false);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("emotion-count", config, builder.createTopology());
        Thread.sleep(100000);
        cluster.shutdown();
    }
}
