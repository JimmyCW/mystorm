package com.wx.mystorm.all;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author weixing
 * @date 2019/2/24
 **/
public class AllFirstBolt extends BaseBasicBolt {

    private final static Logger logger = LoggerFactory.getLogger(AllFirstBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String name = tuple.getStringByField("name");
        logger.info("all first bolt execute name:{}", name);
//        basicOutputCollector.emit(new Values(name.toUpperCase()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("upperName"));
    }

    public static void main(String[] args) {

    }
}
