package com.wx.mystorm.randomstring;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author weixing
 * @date 2019/2/23
 **/
public class RandomStringTopology {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("RandomStringSpout", new RandomStringSpout());
        topologyBuilder.setBolt("WrapStarBolt", new WrapStarBolt(), 4).shuffleGrouping("RandomStringSpout");
        topologyBuilder.setBolt("WrapWellBolt", new WrapWellBolt(), 4).shuffleGrouping("RandomStringSpout");

        Config config = new Config();
        config.setDebug(true);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("RandomStringTopology", config, topologyBuilder.createTopology());
        System.out.println("the first topology is running");

        TimeUnit.SECONDS.sleep(30);
        localCluster.killTopology("RandomStringTopology");
        localCluster.shutdown();
    }
}
