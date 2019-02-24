package com.wx.mystorm.randomstring;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author weixing
 * @date 2019/2/23
 **/
public class RandomStringTopologyRemote {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("RandomStringSpout", new RandomStringSpout());
        topologyBuilder.setBolt("WrapStarBolt", new WrapStarBolt(), 4).shuffleGrouping("RandomStringSpout");
        topologyBuilder.setBolt("WrapWellBolt", new WrapWellBolt(), 4).shuffleGrouping("RandomStringSpout");

        Config config = new Config();
        config.setNumWorkers(3);

        try {
            StormSubmitter.submitTopology("RandomStringTopologyRemote", config, topologyBuilder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
}
