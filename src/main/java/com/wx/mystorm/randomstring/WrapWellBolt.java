package com.wx.mystorm.randomstring;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author weixing
 * @date 2019/2/23
 **/
public class WrapWellBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String stream = tuple.getStringByField("stream");
        System.out.println("#########" + stream + "########");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
