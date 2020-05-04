package com.scott.eshop.storm;

import com.scott.eshop.storm.bolt.LogParseBolt;
import com.scott.eshop.storm.bolt.ProductCountBolt;
import com.scott.eshop.storm.spout.AccessLogKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 热数据统计拓扑
 * @ClassName HotProductTopology
 * @Description
 * @Author 47980
 * @Date 2020/5/4 16:39
 * @Version V_1.0
 **/
public class HotProductTopology {

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("AccessLogKafkaSpout", new AccessLogKafkaSpout(), 1);
        topologyBuilder.setBolt("LogParseBolt", new LogParseBolt(), 2)
                .setNumTasks(2)
                .shuffleGrouping("AccessLogKafkaSpout");
        topologyBuilder.setBolt("ProductCountBolt", new ProductCountBolt(), 2)
                .setNumTasks(2)
                .fieldsGrouping("LogParseBolt", new Fields("productId"));

        Config config = new Config();

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);

            try {
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HotProductTopology", config, topologyBuilder.createTopology());
            Utils.sleep(30000);
            cluster.shutdown();
        }


    }

}
