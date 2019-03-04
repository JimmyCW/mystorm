package com.wx.mystorm.review;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author weixing
 * @date 2019/3/2
 **/
public class StringRandomBolt extends BaseBasicBolt {


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String stringrandom = tuple.getStringByField("stringrandom");
        System.out.println("kkkkkkkkkkkkkkkk = "
                + stringrandom + " thread : " + Thread.currentThread().getName());
        basicOutputCollector.emit(new Values(stringrandom + " again"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("stringrandomagain"));
    }
}
