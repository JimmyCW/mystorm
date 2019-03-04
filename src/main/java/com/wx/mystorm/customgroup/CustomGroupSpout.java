package com.wx.mystorm.customgroup;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author weixing
 * @date 2019/3/3
 **/
public class CustomGroupSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;

    private TopologyContext topologyContext;

    private List<String> strings;

    private AtomicInteger atomicInteger;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.topologyContext = topologyContext;
        strings = Arrays.asList("JAVA.IO",
                "JAVE.MULTITHREAD",
                "SCALA.AKKA",
                "SCALA.OK",
                "BIGDATA.STORM",
                "BIGDATA.SPARK");
        atomicInteger = new AtomicInteger(0);
    }

    @Override
    public void nextTuple() {
        int index = atomicInteger.getAndIncrement();
        if(index < strings.size()) {
            spoutOutputCollector.emit(new Values(index));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("know"));
    }
}
