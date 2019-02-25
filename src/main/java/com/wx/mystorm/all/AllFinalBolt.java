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
public class AllFinalBolt extends BaseBasicBolt {
    private final static Logger logger = LoggerFactory.getLogger(AllFinalBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String name = tuple.getStringByField("name");
        logger.info("all final bolt name:{}", name);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
