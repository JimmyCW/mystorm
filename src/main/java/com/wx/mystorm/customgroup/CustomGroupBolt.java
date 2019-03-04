package com.wx.mystorm.customgroup;

import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Spliterators;

/**
 * @author weixing
 * @date 2019/3/3
 **/
public class CustomGroupBolt extends BaseBasicBolt {

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.context = context;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String know = tuple.getStringByField("know");
        List<String> list = Splitter.on(".").splitToList(know);
        basicOutputCollector.emit(new Values(list.get(0), list.get(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lan", "use"));
    }
}
