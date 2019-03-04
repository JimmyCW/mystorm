package com.wx.mystorm.customgroup;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author weixing
 * @date 2019/3/3
 **/
public class CustomGroupPrintBolt extends BaseBasicBolt {

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.context = context;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String lan = tuple.getStringByField("lan");
        String use = tuple.getStringByField("use");
        System.out.println("print lan:" + lan + " use:" + use);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
