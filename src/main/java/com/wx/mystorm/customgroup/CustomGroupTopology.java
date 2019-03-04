package com.wx.mystorm.customgroup;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author weixing
 * @date 2019/3/3
 **/
public class CustomGroupTopology {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("CustomGroupSpout", new CustomGroupSpout(), 1);
        topologyBuilder.setBolt("CustomGroupBolt", new CustomGroupBolt(), 2).localOrShuffleGrouping("CustomGroupSpout");
        topologyBuilder.setBolt("CustomGroupPrintBolt", new CustomGroupPrintBolt(), 2).customGrouping("CustomGroupBolt", new MyCustomGroup());

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(5);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("CustomGroupTopology", config, topologyBuilder.createTopology());

        TimeUnit.SECONDS.sleep(30);

        localCluster.killTopology("CustomGroupTopology");
        localCluster.shutdown();
    }
}
