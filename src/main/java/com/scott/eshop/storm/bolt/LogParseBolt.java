package com.scott.eshop.storm.bolt;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 日志解析的 bolt
 * @ClassName LogParseBolt
 * @Description
 * @Author 47980
 * @Date 2020/5/4 17:10
 * @Version V_1.0
 **/
public class LogParseBolt extends BaseRichBolt {

    private OutputCollector collector;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogParseBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String message = tuple.getStringByField("message");

        LOGGER.info("【LogParseBolt 接收到一条日志】 message="+message);

        JSONObject messageJSON = JSONObject.parseObject(message);
        JSONObject uriArgsJSON = messageJSON.getJSONObject("uri_args");
        Long productId = uriArgsJSON.getLong("productId");

        if (productId != null) {
            collector.emit(new Values(productId));
            LOGGER.info("【LogParseBolt 发射到一条日志】 productId="+productId);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("productId"));
    }
}
