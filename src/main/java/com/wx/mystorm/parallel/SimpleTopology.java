package com.wx.mystorm.parallel;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author weixing
 * @date 2019/2/24
 **/
public class SimpleTopology {
    private final static Logger logger = LoggerFactory.getLogger(SimpleTopology.class);
    //topology name
    //component - prefix
    //workers
    //spout executor size parallel hint
    //spout task
    //bolt executor size parallel hint
    //bolt task
    public static void main(String[] args) {
        if(args.length != 7) {
            throw new IllegalArgumentException("args length error");
        }

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Args arg = new Args(args[0], args[1], Integer.parseInt(args[2]),
                Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Integer.parseInt(args[6]));
        String spoutName = arg.getComponentPrefix() + "spout";
        topologyBuilder.setSpout(spoutName, new SimpleSpout(), arg.getSpoutParallelHint()).setNumTasks(arg.getSpoutNumTask());
        topologyBuilder.setBolt(arg.getComponentPrefix() + "bolt", new SimpleBolt(), arg.getBoltParallelHint()).setNumTasks(arg.getBoltNumTask()).shuffleGrouping(spoutName);
//        topologyBuilder.setBolt(arg.getComponentPrefix() + "bolt", new SimpleBolt()).fieldsGrouping("前一个boltid", new Fields("前一个field"));
        Config config = new Config();
        config.setNumWorkers(arg.getWorkNums());

        try {
            StormSubmitter.submitTopology(arg.topologyName, config, topologyBuilder.createTopology());
            logger.info("============================");
            logger.info("topology submit");
            logger.info("============================");
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
    private static class Args {
        private String topologyName;
        private String componentPrefix;
        private int workNums;
        private int spoutParallelHint;
        private int spoutNumTask;
        private int boltParallelHint;
        private int boltNumTask;

        public Args(String topologyName, String componentPrefix, int workNums, int spoutParallelHint, int spoutNumTask, int boltParallelHint, int boltNumTask) {
            this.topologyName = topologyName;
            this.componentPrefix = componentPrefix;
            this.workNums = workNums;
            this.spoutParallelHint = spoutParallelHint;
            this.spoutNumTask = spoutNumTask;
            this.boltParallelHint = boltParallelHint;
            this.boltNumTask = boltNumTask;
        }

        public String getTopologyName() {
            return topologyName;
        }

        public String getComponentPrefix() {
            return componentPrefix;
        }

        public int getWorkNums() {
            return workNums;
        }

        public int getSpoutParallelHint() {
            return spoutParallelHint;
        }

        public int getSpoutNumTask() {
            return spoutNumTask;
        }

        public int getBoltParallelHint() {
            return boltParallelHint;
        }

        public int getBoltNumTask() {
            return boltNumTask;
        }

        @Override
        public String toString() {
            return "Args{" +
                    "topologyName='" + topologyName + '\'' +
                    ", componentPrefix='" + componentPrefix + '\'' +
                    ", workNums=" + workNums +
                    ", spoutParallelHint=" + spoutParallelHint +
                    ", spoutNumTask=" + spoutNumTask +
                    ", boltParallelHint=" + boltParallelHint +
                    ", boltNumTask=" + boltNumTask +
                    '}';
        }
    }
}
