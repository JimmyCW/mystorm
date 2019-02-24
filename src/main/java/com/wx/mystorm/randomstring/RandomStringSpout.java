package com.wx.mystorm.randomstring;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author weixing
 * @date 2019/2/23
 **/
public class RandomStringSpout extends BaseRichSpout {

    private static final Map<Integer, String> map = new HashMap<>();

    static {
        map.put(0, "APACHE SPARK");
        map.put(1, "KAFKA STREAMING");
        map.put(2, "APACHE NIFI");
        map.put(3, "APACHE FLINK");
        map.put(4, "APACHE STORM");
    }

    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.out.println("========== spout open ===========");
        //集合赋值，赋值后发射到next tuple
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        //发送数据, 发送数据到哪个fields 还需要定义一下@see declareOutputFields
        spoutOutputCollector.emit(new Values(map.get(current().nextInt(5))));

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //发送到这个field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stream"));
    }
}
