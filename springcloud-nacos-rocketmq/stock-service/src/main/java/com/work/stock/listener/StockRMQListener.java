package com.work.stock.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.work.stock.service.StockService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author admin
 * @description <TODO description class purpose>
 * @date 2023/5/10
 */
@Slf4j
@Component
@RocketMQMessageListener(consumerGroup = "stock-tx-consume-group", topic = "order-tx-RMQ_TOPIC")
public class StockRMQListener implements RocketMQListener<String> {

    @Resource
    private StockService stockService;


    @Override
    public void onMessage(String msg) {
        log.info("received message: {}", msg);
        JSONObject object = JSON.parseObject(msg);
        String commodityCode = object.getString("commodityCode");
        int count = object.getInteger("count");
        stockService.deduct(commodityCode, count);
        log.info("deduct stock success");
    }
}

