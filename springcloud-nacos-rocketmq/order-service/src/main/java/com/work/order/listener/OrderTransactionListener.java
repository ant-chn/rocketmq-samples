package com.work.order.listener;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.work.order.model.Order;
import com.work.order.repository.OrderDAO;
import com.work.order.service.OrderService;
import com.work.order.service.TxOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import javax.annotation.Resource;

/**
 * @author zlt
 */
@Component
@Slf4j
@RocketMQTransactionListener
public class OrderTransactionListener implements RocketMQLocalTransactionListener {

	@Resource
	private TxOrderService txorderService;
	@Resource
	private OrderDAO orderDAO;


    /**
     * 提交本地事务
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object obj) {
        log.info("执行本地事务,executeLocalTransaction");
        StopWatch stopWatch = new StopWatch("执行本地事务");
        stopWatch.start("1. 读取mq消息");
        //获取事务ID
        MessageHeaders headers = message.getHeaders();
        String transactionId = (String) headers.get(RocketMQHeaders.TRANSACTION_ID);
        String orderNo = (String) headers.get("order_no");
        log.info("transactionId is {}, orderNo is {}", transactionId, orderNo);
        stopWatch.stop();

        try{
            stopWatch.start("2.订单信息保存到mysql中");
            Order order = (Order) obj;
            //执行本地事务，并记录日志
            txorderService.doCreate(order);
            stopWatch.stop();
            //执行成功，可以提交事务
        }catch (Exception e){
            e.printStackTrace();
            log.info("本地事务执行失败，回滚消息");
            return RocketMQLocalTransactionState.ROLLBACK;
        }
        log.info(stopWatch.prettyPrint());
        //提交事务消息
        return RocketMQLocalTransactionState.COMMIT;
    }

    /**
     * 本地事务的检查，检查本地事务是否成功
     *
     * 如果事务消息一直没提交，则定时判断订单数据是否已经插入
     *     是：提交事务消息
     *     否：回滚事务消息
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        log.info("事务监听 - 回查事务状态");
        StopWatch stopWatch = new StopWatch("回查事务表checkLocalTransaction：message");
        stopWatch.start("1. 转化mq消息");
        MessageHeaders headers = message.getHeaders();
        String transactionId = (String) headers.get(RocketMQHeaders.TRANSACTION_ID);
        String orderNo = (String) headers.get("order_no");
        log.info("检查本地事务,transactionId is {}, orderNo is {}", transactionId, orderNo);
        stopWatch.stop();

        stopWatch.start("2.事务回查判断事务是否提交该事务：");
        Integer count = orderDAO.selectCount(Wrappers.<Order>lambdaQuery().eq(Order::getOrderNo, orderNo));
        //判断之前的事务是否已经提交：订单记录是否已经保存
        stopWatch.stop();
        log.info(String.format("============事务回查-订单已生成-orderNo:{}-提交事务消息", orderNo));
        log.info(stopWatch.prettyPrint());
        return count > 0 ? RocketMQLocalTransactionState.COMMIT : RocketMQLocalTransactionState.ROLLBACK;
    }
}