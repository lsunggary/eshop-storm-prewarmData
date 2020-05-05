package com.scott.eshop.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.scott.eshop.storm.http.HttpClientUtils;
import com.scott.eshop.storm.zk.ZookeeperSession;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.storm.shade.org.apache.http.protocol.HTTP;
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
        new Thread(new HotProductFindThread()).start();
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

    private class HotProductFindThread implements Runnable{

        @Override
        public void run() {
            while(true) {
                // 1. 先做全局的排序
                List<Map.Entry<Long, Long>> productList = new ArrayList<Map.Entry<Long, Long>>();
                List<Long> hotProductIdList = new ArrayList<Long>();
                List<Long> lastTimeHotProductIdList = new ArrayList<Long>();
                // 将LRUMap中的数据按照访问次数进行全局排序
                // 计算95%的商品的访问次数的平均值
                // 遍历排序后的商品访问次数，最大的开始
                // 如果某个商品的访问量时平均值的10倍，就认为时热点数据。
                try {
                    productList.clear();
                    hotProductIdList.clear();

                    if (productCountMap.size() == 0) {
                        Utils.sleep(100);
                        continue;
                    }

                    LOGGER.info("【ProductCountBolt  热点发现topnProductList的长度】 size="+productList.size());

                    for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {

                        // 边界0 把当前数据加入集合
                        if (productList.size() == 0) {
                            productList.add(productCountEntry);
                        } else {
                            boolean bigger = false;
                            // 对 topnProductList 遍历，比较每个的value和 productCountEntry 的value的大小
                            for (int i = 0; i < productList.size(); i++) {
                                Map.Entry<Long, Long> topnProductCountEntry = productList.get(i);
                                if (productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                    // 如果 productCountEntry 值比当前值大
                                    // 把当前值的位置逐个往后替换
                                    // 然后加入 productCountEntry 到当前位置
                                    int lastIndex = productList.size() < productCountMap.size() ? productList.size() - 1 : productCountMap.size() - 2;
                                    for (int j = lastIndex; j >= i; j--) {
                                        // 计算边界的时候，增加占位符
                                        if (j + 1 == productList.size()) {
                                            productList.add(null);
                                        }
                                        productList.set(j + 1, productList.get(j));
                                    }
                                    productList.set(i, productCountEntry);
                                    bigger = true;
                                    break;
                                }
                            }

                            // 如果 productCountEntry 比集合中所有的数都小
                            // 并且集合的长度小于设定的值，就把当前 productCountEntry 放入集合。
                            if (!bigger) {
                                if (productList.size() < productCountMap.size()) {
                                    productList.add(productCountEntry);
                                }
                            }
                        }
                    }

                    LOGGER.info("【HotProductFindThread 全局排序后的商品访问结果】 productList="+productList);

                    // 2. 计算出95%的商品访问的平均值
                    int calculateCount = (int)Math.floor(productList.size() * 0.95);

                    Long totalCount = 0L;

                    for (int i = productList.size() - 1; i >= productList.size() - calculateCount; i--) {
                        totalCount += productList.get(i).getValue();
                    }
                    Long avgCount = totalCount / calculateCount;
                    LOGGER.info("【HotProductFindThread 计算后95%的商品访问次数的平均数】 avgCount="+avgCount);

                    // 3. 从第一个数据开始遍历，是否为10倍
                    for(Map.Entry<Long, Long> productCountEntry : productList) {
                        if (productCountEntry.getValue() > 10 * avgCount) {
                            LOGGER.info("【HotProductFindThread 发现一个热点】 productCountEntry="+productCountEntry);
                            hotProductIdList.add(productCountEntry.getKey());

                            if (!lastTimeHotProductIdList.contains(productCountEntry.getKey())) {
                                // 将缓存热点反向推送到流量分发的nginx中
                                String distributeURL = "http://192.168.52.107/hot?productId=" + productCountEntry.getKey();
                                HttpClientUtils.sendGetRequest(distributeURL);
                                // 将缓存热点的完整缓存数据，发送请求到缓存服务去获取，然后发送到应用nginx上
                                String cacheServiceURL = "http://192.168.52.109:8081/getProductInfo?productId=" + productCountEntry.getKey();
                                String responce = HttpClientUtils.sendGetRequest(cacheServiceURL);
                                List<NameValuePair> params = new ArrayList<NameValuePair>();
                                params.add(new BasicNameValuePair("productInfo", responce));
                                String productInfo = URLEncodedUtils.format(params, HTTP.UTF_8);

                                String[] appNginxURLs = new String[]{
                                        "http://192.168.52.115/hot?productId="+productCountEntry.getKey()+"&"+productInfo,
                                        "http://192.168.52.113/hot?productId="+productCountEntry.getKey()+"&"+productInfo
                                };

                                for (String appNginxURL : appNginxURLs) {
                                    LOGGER.info("【HotProductFindThread 发送热点到应用nginx】 appNginxURL="+appNginxURL);
                                    HttpClientUtils.sendGetRequest(appNginxURL);
                                }
                            }
                        }
                    }

                    // 4. 实时感知热点数据的消失
                    if (lastTimeHotProductIdList.size() == 0) {
                        if (hotProductIdList.size() > 0) {
                            for (Long productId : hotProductIdList) {
                                lastTimeHotProductIdList.add(productId);
                            }
                            LOGGER.info("【HotProductFindThread 保存上次热点数据】 lastTimeHotProductIdList="+lastTimeHotProductIdList);
                        }
                    } else {
                        for (Long productId : lastTimeHotProductIdList) {
                            if (!hotProductIdList.contains(productId)) {
                                LOGGER.info("【HotProductFindThread 发下一个热点消失了】 productId="+productId);
                                // 说明上次的商品id的热点，消失了
                                // 发送一次http请求给流量分发的nginx中，取消热点缓存的标识
                                String url = "http://192.168.52.107/cancel_hot?productId"+productId;
                                HttpClientUtils.sendGetRequest(url);
                            }
                        }

                        if (hotProductIdList.size() > 0) {
                            lastTimeHotProductIdList.clear();
                            for (Long productId : hotProductIdList) {
                                lastTimeHotProductIdList.add(productId);
                            }
                            LOGGER.info("【HotProductFindThread 保存上次热点数据】 lastTimeHotProductIdList="+lastTimeHotProductIdList);
                        } else {
                            lastTimeHotProductIdList.clear();
                        }
                    }

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
