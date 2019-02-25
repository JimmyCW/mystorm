package com.wx.mystorm.all;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author weixing
 * @date 2019/2/24
 **/
public class StringAllSpout extends BaseRichSpout {

    private final static Logger logger = LoggerFactory.getLogger(StringAllSpout.class);

    private SpoutOutputCollector spoutOutputCollector;

    private List<String> data;

    private AtomicInteger index;

    private TopologyContext topologyContext;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.topologyContext = topologyContext;
        data = Arrays.asList("Wei", "Wei", "Wei", "Wei", "Wei",
                "Wei", "Xing", "Xing", "Xing", "Xing");
        index = new AtomicInteger();
    }

    @Override
    public void nextTuple() {
        int ai = index.getAndIncrement();
        if(ai < 10) {
            spoutOutputCollector.emit(new Values(data.get(ai)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("name"));
    }
}
