package com.wx.mystorm.review;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author weixing
 * @date 2019/3/2
 **/
public class StringRandomSecondBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String stringrandom = tuple.getStringByField("stringrandomagain");
        System.out.println("llllllllllllll = "
                + stringrandom + " thread : " + Thread.currentThread().getName());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
