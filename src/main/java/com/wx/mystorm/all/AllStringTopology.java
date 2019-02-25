package com.wx.mystorm.all;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author weixing
 * @date 2019/2/24
 **/
public class AllStringTopology {

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("StringAllSpout", new StringAllSpout(), 1);
        topologyBuilder.setBolt("AllFirstBolt", new AllFirstBolt(), 2).fieldsGrouping("StringAllSpout", new Fields("name"));
        topologyBuilder.setBolt("AllFinalBolt", new AllFinalBolt(), 2).fieldsGrouping("StringAllSpout", new Fields("name"));

        Config config = new Config();
        config.setNumWorkers(2);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("AllStringTopology", config, topologyBuilder.createTopology());
        System.out.println("submit ==========================================");

    }

}
