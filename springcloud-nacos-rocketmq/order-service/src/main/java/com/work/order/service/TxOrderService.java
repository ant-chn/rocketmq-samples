/*
 *  Copyright 1999-2021 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.work.order.service;

import com.alibaba.fastjson.JSON;
import com.work.order.feign.AccountFeignClient;
import com.work.order.feign.StockFeignClient;
import com.work.order.model.Order;
import com.work.order.repository.OrderDAO;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Program Name: springcloud-nacos-seata
 * <p>
 * Description:
 * <p>
 *
 * @author zhangjianwei
 * @version 1.0
 * @date 2019/8/28 4:05 PM
 */
@Slf4j
@Service
public class TxOrderService {

    @Resource
    private OrderDAO orderDAO;
    @Resource
    RocketMQTemplate rocketMQTemplate;
    @Resource
    RedisTemplate<String, Object> redisTemplate;
    public static final String TOPIC = "order-tx-RMQ_TOPIC";
    public static final String rediskey = "order-key:";

    /**
     * 下单：创建订单、减库存，涉及到两个服务
     * 创建订单的业务方法
     * 这里修改为：只向 Rocketmq 发送事务消息。
     * @param userId
     * @param commodityCode
     * @param count
     */
    public void placeTxOrder(String userId, String commodityCode, Integer count) {
        BigDecimal orderMoney = new BigDecimal(count).multiply(new BigDecimal(5));
        String orderNo = UUID.randomUUID().toString();
        Order order = new Order().setOrderNo(orderNo).setUserId(userId)
                .setCommodityCode(commodityCode).setCount(count).setMoney(orderMoney);
        //如果可以创建订单则发送消息给rocketmq，让用户中心消费消息
        String transactionId = UUID.randomUUID().toString();
        Message<String> msg = MessageBuilder.withPayload(JSON.toJSONString(order))
                .setHeader(RocketMQHeaders.TRANSACTION_ID, transactionId)
                .setHeader("order_no", orderNo).build();
        TransactionSendResult sendResult = rocketMQTemplate.sendMessageInTransaction(TOPIC + ":tagX", msg, order);
        log.info("【sendMsg】sendResult={}", JSON.toJSONString(sendResult));
    }

    /**
     * 本地事务，执行订单保存
     * 这个方法在事务监听器中调用
     * @param order
     */
    @Transactional(rollbackFor = RuntimeException.class)
    public void doCreate(Order order) {
        BigDecimal orderMoney = new BigDecimal(order.getCount()).multiply(new BigDecimal(5));
        order.setMoney(orderMoney);
        orderDAO.insert(order);
        redisTemplate.opsForValue().set(rediskey+order.getOrderNo(), JSON.toJSONString(order), 5000L);
    }

}
