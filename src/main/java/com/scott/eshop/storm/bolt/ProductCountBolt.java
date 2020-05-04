package com.scott.eshop.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.scott.eshop.storm.zk.ZookeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 商品访问次数，统计bolt
 * @ClassName ProductCountBolt
 * @Description
 * @Author 47980
 * @Date 2020/5/4 17:14
 * @Version V_1.0
 **/
public class ProductCountBolt  extends BaseRichBolt {

    /**
     * 使用 LRU 算法自动清理数据的 map
     * 保存最近访问的数据给存储下来
     * 根据内存和数据量算出的数字
     * 这里给1000
     */
    private LRUMap<Long, Long> productCountMap = new LRUMap<Long, Long>(1000);
    private ZookeeperSession zkSession;
    private int currentTaskid;

    private static final Logger LOGGER  = LoggerFactory.getLogger(ProductCountBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        new Thread(new ProductCountThread()).start();
        zkSession = ZookeeperSession.getInstance();
        currentTaskid = topologyContext.getThisTaskId();
        // 1. 将自己的taskid写入一个zookeeper node中，形成taskid列表
        // 2. 然后每次都自己的热门商品列表，写入自己的taskid对应的zookeeper节点
        // 3. 然后并行的预热程序才能从第一步中知道有哪些taskid
        // 4. 然后并行预热程序根据每个taskid去获取一个锁，再从对应的znode中拿到热门商品列表
        initTaskId(currentTaskid);
    }

    /**
     * ProductCountBolt 所有的task启动的时候，都会将自己的taskid写到同一个node的值中
     * 格式就是逗号分隔。
     * 111,222,333
     * @param taskid
     */
    private void initTaskId(int taskid) {
        zkSession.acquireDistributeLock();

        zkSession.createNode("/taskid-list");
        String taskidList = zkSession.getNodeData();

        LOGGER.info("【ProductCountBolt 获取到taskid list】 taskidList="+taskidList);

        if (!"".equals(taskidList)) {
            taskidList += "," + taskid;
        } else {
            taskidList += taskid;
        }
        zkSession.setNodeData("/taskid-list", taskidList);
        LOGGER.info("【ProductCountBolt 设置taskid list】 taskidList="+taskidList);

        zkSession.releaseDistributeLock();
    }

    private class ProductCountThread implements Runnable {

        @Override
        public void run() {
            List<Map.Entry<Long, Long>> topnProductList = new ArrayList<Map.Entry<Long, Long>>();
            List<Long> topnProductResult = new ArrayList<Long>();

            while (true) {
                try {
                    topnProductList.clear();
                    topnProductResult.clear();

                    int topn = 3;

                    if (productCountMap.size() == 0) {
                        Utils.sleep(100);
                        continue;
                    }

                    LOGGER.info("【ProductCountBolt 发现topnProductList的长度】 size="+topnProductList.size());

                    for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {

                        // 边界0 把当前数据加入集合
                        if (topnProductList.size() == 0) {
                            topnProductList.add(productCountEntry);
                        } else {
                            boolean bigger = false;
                            // 对 topnProductList 遍历，比较每个的value和 productCountEntry 的value的大小
                            for (int i = 0; i < topnProductList.size(); i++) {
                                Map.Entry<Long, Long> topnProductCountEntry = topnProductList.get(i);
                                if (productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                    // 如果 productCountEntry 值比当前值大
                                    // 把当前值的位置逐个往后替换
                                    // 然后加入 productCountEntry 到当前位置
                                    int lastIndex = topnProductList.size() < topn ? topnProductList.size() - 1 : topn - 2;
                                    for (int j = lastIndex; j >= i; j--) {
                                        // 计算边界的时候，增加占位符
                                        if (j + 1 == topnProductList.size()) {
                                            topnProductList.add(null);
                                        }
                                        topnProductList.set(j + 1, topnProductList.get(j));
                                    }
                                    topnProductList.set(i, productCountEntry);
                                    bigger = true;
                                    break;
                                }
                            }

                            // 如果 productCountEntry 比集合中所有的数都小
                            // 并且集合的长度小于设定的值，就把当前 productCountEntry 放入集合。
                            if (!bigger) {
                                if (topnProductList.size() < topn) {
                                    topnProductList.add(productCountEntry);
                                }
                            }
                        }
                    }

                    // 获取到topn list
                    for (Map.Entry<Long, Long> topnProduct : topnProductList) {
                        topnProductResult.add(topnProduct.getKey());
                    }

                    String topnProductListJSON = JSONArray.toJSONString(topnProductResult);
                    zkSession.createNode("/task-hot-product-list-"+currentTaskid);
                    zkSession.setNodeData("/task-hot-product-list-"+currentTaskid, topnProductListJSON);
                    LOGGER.info("【ProductCountBolt 计算出一份top3热门商品列表】 zk path=/task-hot-product-list-"+currentTaskid+", topnProductListJSON="+topnProductListJSON);
                    Utils.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");

        LOGGER.info("【ProductCountBolt 接收到一个商品id】 productId="+productId);
        Long count = productCountMap.get(productId);
        if (count == null) {
            count = 0L;
        }
        count++;

        productCountMap.put(productId, count);
        LOGGER.info("【ProductCountBolt 完成商品次数统计】 productId="+productId+ ", count="+ count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
