package com.wx.mystorm.parallel;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author weixing
 * @date 2019/2/24
 **/
public class SimpleBolt extends BaseBasicBolt {

    private final static Logger logger = LoggerFactory.getLogger(SimpleSpout.class);

    private TopologyContext topologyContext;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        logger.info("simple bolt  -> prepare, hashcode:{}, thread:{}, taskId:{}", this.hashCode(), Thread.currentThread(), context.getThisTaskId());
        this.topologyContext = context;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Integer parallel = tuple.getIntegerByField("parallel");
        logger.info("simple bolt  -> execute, hashcode:{}, thread:{}, taskId:{}, int:{}", this.hashCode(), Thread.currentThread(), topologyContext.getThisTaskId(), parallel);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        logger.info("simple bolt  -> cleanup, hashcode:{}, thread:{}, taskId:{}", this.hashCode(), Thread.currentThread(), topologyContext.getThisTaskId());
    }
}
