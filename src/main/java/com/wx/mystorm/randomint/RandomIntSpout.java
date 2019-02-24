package com.wx.mystorm.randomint;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author weixing
 * @date 2019/2/23
 **/
public class RandomIntSpout extends BaseRichSpout {

    private static final List<Integer> integers = new ArrayList<>(5);

    static {
        integers.add(0);
        integers.add(1);
        integers.add(2);
        integers.add(3);
        integers.add(4);
    }

    private SpoutOutputCollector spoutOutputCollector ;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.out.println("the int spout open");
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(new Values(integers.get(current().nextInt(5))));

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("intStream"));
    }
}
