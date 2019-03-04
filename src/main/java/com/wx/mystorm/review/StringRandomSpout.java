package com.wx.mystorm.review;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author weixing
 * @date 2019/3/2
 **/
public class StringRandomSpout extends BaseRichSpout {

    private TopologyContext topologyContext;

    private SpoutOutputCollector spoutOutputCollector;

    private List<String> list;

    private AtomicInteger atomicInteger;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.topologyContext = topologyContext;
        list = Arrays.asList("jimmy", "wei", "jimmy", "wei", "jimmy", "wei");
        atomicInteger = new AtomicInteger();
    }

    @Override
    public void nextTuple() {
        int andIncrement = atomicInteger.getAndIncrement();
        if(andIncrement <= 20) {
            spoutOutputCollector.emit(new Values(list.get(current().nextInt(6))));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("stringrandom"));
    }

    public static void main(String[] args) {
        for (int i = 0; i < 50; i++) {
            System.out.print(current().nextInt(6));
        }
    }
}
