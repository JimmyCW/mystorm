package com.wx.mystorm.randomint;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author weixing
 * @date 2019/2/23
 **/
public class RandomIntTopology {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("randomIntSpout", new RandomIntSpout());
        topologyBuilder.setBolt("minusBolt", new MinusBolt()).shuffleGrouping("randomIntSpout");
        topologyBuilder.setBolt("plusBolt", new PlusBolt()).shuffleGrouping("randomIntSpout");

        Config config = new Config();
        config.setDebug(true);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("randomIntTopology", config, topologyBuilder.createTopology());

        TimeUnit.SECONDS.sleep(20);

        localCluster.killTopology("randomIntTopology");
        localCluster.shutdown();
    }
}
