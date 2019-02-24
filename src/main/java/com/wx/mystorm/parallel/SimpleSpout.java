package com.wx.mystorm.parallel;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author weixing
 * @date 2019/2/24
 **/
public class SimpleSpout extends BaseRichSpout {

    private final static Logger logger = LoggerFactory.getLogger(SimpleSpout.class);

    private TopologyContext topologyContext;

    private SpoutOutputCollector spoutOutputCollector;

    private AtomicInteger atomicInteger;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.topologyContext = topologyContext;
        atomicInteger = new AtomicInteger();
        logger.info("simple spout -> open, hashcode:{}, thread:{}, taskId:{}", this.hashCode(), Thread.currentThread(), topologyContext.getThisTaskId());
    }

    @Override
    public void nextTuple() {
        int ai = atomicInteger.incrementAndGet();
        if(ai <= 10) {
            logger.info("simple spout -> nextTuple, hashcode:{}, thread:{}, taskId:{}, int:{}", this.hashCode(), Thread.currentThread(), topologyContext.getThisTaskId(), ai);
            spoutOutputCollector.emit(new Values(ai));
        }

        try {
            TimeUnit.SECONDS.sleep(20);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("parallel"));
    }

    @Override
    public void close() {
        logger.info("simple spout -> close, hashcode:{}, thread:{}, taskId:{}", this.hashCode(), Thread.currentThread(), topologyContext.getThisTaskId());
    }
}
