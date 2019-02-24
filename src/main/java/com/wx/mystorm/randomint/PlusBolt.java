package com.wx.mystorm.randomint;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author weixing
 * @date 2019/2/23
 **/
public class PlusBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int a = tuple.getIntegerByField("intStream").intValue();
        System.out.println("plus bolt a = " + a);
        int b = tuple.getIntegerByField("intStream").intValue();
        System.out.println("plus bolt b = " + b);
        System.out.println(" a + b = " + (a + b));
        basicOutputCollector.emit(new Values(a + b));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("intStream"));

    }
}
