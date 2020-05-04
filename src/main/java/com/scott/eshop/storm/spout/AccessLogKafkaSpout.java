package com.scott.eshop.storm.spout;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Kafka消费数据的spout
 * @ClassName AccessLogKafkaSpout
 * @Description
 * @Author 47980
 * @Date 2020/5/4 16:58
 * @Version V_1.0
 **/

public class AccessLogKafkaSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogKafkaSpout.class);

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        startKafkaConsumer();
    }

    /**
     * 建立kafka连接
     */
    private void startKafkaConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.52.115:2181,192.168.52.113:2181,192.168.52.107:2181");
        props.put("group.id", "eshop-cache-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        String topic = "access-log";

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for (final KafkaStream stream: streams) {
                new Thread(new KafkaMessageProcessor(stream)).start();
        }
    }

    /**
     * 建立一个内部类线程
     */
    private class KafkaMessageProcessor implements Runnable {

        private KafkaStream kafkaStream;

        public KafkaMessageProcessor (KafkaStream kafkaStream) {
            this.kafkaStream = kafkaStream;
        }

        /**
         * 无线循环拿数据，把数据放在queue中
         * queue是线程安全的。
         */
        @Override
        public void run() {
            ConsumerIterator iterator = kafkaStream.iterator();
            while (iterator.hasNext()) {
                String message = new String((byte[]) iterator.next().message());
                LOGGER.info("【AccessLogKafkaSpout 接受到一条日志】 message="+message);
                try {
                    queue.put(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 从queue中获取数据，然后发送到bolt
     */
    @Override
    public void nextTuple() {
        if (queue.size() > 0) {
            try {
                String message = queue.take();
                collector.emit(new Values(message));
                LOGGER.info("【AccessLogKafkaSpout 发射一条日志】 message="+message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            Utils.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
