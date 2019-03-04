package com.wx.mystorm.review;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * @author weixing
 * @date 2019/3/2
 **/
public class StringRandomTopology {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("StringRandomSpout", new StringRandomSpout(), 1);
        topologyBuilder.setBolt("StringRandomBolt", new StringRandomBolt(), 2).fieldsGrouping("StringRandomSpout", new Fields("stringrandom")).setNumTasks(2);
        topologyBuilder.setBolt("StringRandomSecondBolt", new StringRandomSecondBolt(), 2).globalGrouping("StringRandomBolt");

        Config config = new Config();
        config.setNumWorkers(4);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("StringRandomTopology", config, topologyBuilder.createTopology());

    }
}
