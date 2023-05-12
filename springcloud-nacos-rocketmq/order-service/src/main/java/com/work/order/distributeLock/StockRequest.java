package com.work.order.distributeLock;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.lang.generator.SnowflakeGenerator;
import cn.hutool.core.lang.generator.UUIDGenerator;
import cn.hutool.core.lang.id.NanoId;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.nacos.api.naming.spi.generator.IdGenerator;
import lombok.*;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: central-platform
 * @description: com.rocketmq.demo.ConcurrentHashMapLock.StockRequest.java
 * @Author admin
 * @Date 2021/7/6 12:06
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
class StockRequest implements Serializable {
    //id
    long id;
    //会员id
    String memberId;
    //切分消费数量
    long buyNum;
    //原消费数量
    long originalNum;
}

/**
 * 测试分段锁
 */
class ConcurrentSegmentLockTest implements Runnable {
    // 模拟100个线程并发
    static int segmentLockSize = 100; //分段锁
    static int taskSize = 20000; //任务数量
    static long initstockCapacity = 20000l; //初始化库存
    static SegmentDistributeLock segmentDistributeLock = new SegmentDistributeLock(initstockCapacity, segmentLockSize);
    static ExecutorService executorService = Executors.newFixedThreadPool(64);
    static CountDownLatch countDownLatchSignal;
    int num; //购买数量

    static SnowflakeGenerator snowflakeGenerator = new SnowflakeGenerator();

    public ConcurrentSegmentLockTest(int num) {
        this.num = num;
    }

    @Override
    public void run() {
        try {
            StockRequest request = StockRequest.builder().id(snowflakeGenerator.next())
                    .memberId("memberId_" + RandomUtil.randomNumbers(6))
                    .buyNum(num).originalNum(num).build();
            // 在此阻塞,等到计数器归零之后,再同时开始 扣库存
//            System.out.println(Thread.currentThread().getName() + "已到达, 即将开始扣减库存: "+ this.num);
            if(segmentDistributeLock.handlerStock(request)){
                System.out.println(String.format("%s - %s 扣减成功, 扣减库存为: %d", Thread.currentThread().getName(), Long.toString(snowflakeGenerator.next()), this.num));
            } else {
                System.out.println(String.format("%s - %s 扣减失败, 扣减库存为: %d", Thread.currentThread().getName(), Long.toString(snowflakeGenerator.next()), this.num));
            }
            countDownLatchSignal.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();
        //记录任务执行时间
        long t1 = System.currentTimeMillis();
        CountDownLatch downLatchSignal = new CountDownLatch(1);
        countDownLatchSignal = new CountDownLatch(taskSize);

        // 模拟并发扣减库存(扣减1-50个)
        for (int i = 0; i < taskSize; i++) {
            executorService.submit(new ConcurrentSegmentLockTest(1));
            //阻塞，等待十个任务都执行后，才继续下一批10任务
            //定义线程阻塞为10
            /*if (i != 0 && i % 10 == 0) {
                countDownLatchSignal.await(1, TimeUnit.SECONDS);
                System.out.println("------------------------------------");
                countDownLatchSignal = new CountDownLatch(10);
                setCountDownLatch(countDownLatchSignal);
            }
            countDownLatchSignal.countDown();// 计数减一为0，工作线程真正启动具体操作
             */
        }
        countDownLatchSignal.await();
        downLatchSignal.countDown();
        long t2 = System.currentTimeMillis();
        executorService.shutdown();

        // 并发扣减库存结束,查询最终库存
        Map<Object, Object> consumeobjectMap = segmentDistributeLock.getRedisTemplate().opsForHash().entries("applets:order:consume_sorder");
        Map<Object, Object> actualobjectMap = segmentDistributeLock.getRedisTemplate().opsForHash().entries("applets:order:afail_sorder");
        Map<Object, Object> failobjectMap = segmentDistributeLock.getRedisTemplate().opsForHash().entries("applets:order:fail_sorder");
        long consumesum = consumeobjectMap.values().stream().mapToLong(e -> Long.parseLong((String) e)).sum();
        long actualfialsum = actualobjectMap.values().stream().mapToLong(e -> Long.parseLong((String) e)).sum();
        long fialsum = actualobjectMap.values().stream().mapToLong(e -> Long.parseLong((String) e)).sum();
        System.out.println("=======================================");
        System.out.println("=======================================");
        segmentDistributeLock.showStocks();
        System.out.println("=======================================");
        System.out.println("------------------------------------");
        System.out.println("-----执行时间：" + (t2 - t1)/1000 + "s -----");
        System.out.println(String.format("-----线程数 task: %d, 分布式分段锁 segmentLock: %d, 总库存 stockCapacity: %s-------",
                taskSize, segmentLockSize, StrUtil.toString(initstockCapacity)));
        System.out.println("------------------------------------");
        System.out.println("=======================================");
        System.out.println("-----并发扣减库存结束,查看剩余库存-------");
        System.out.println("=======================================");
        System.out.println(String.format("失败下单数量 size: %d, fConsumeCapacity: %s, afailCapacity:%s", failobjectMap.values().size(), StrUtil.toString(fialsum), StrUtil.toString(actualfialsum)));
        System.out.println("=======================================");
        System.out.println(String.format("总共已下单数量 size: %d, consumeCapacity: %s, lastestCapacity: %s",
                consumeobjectMap.values().size(), StrUtil.toString(consumesum - actualfialsum), StrUtil.toString(segmentDistributeLock.getCurrentTotalCapacity())));
        System.out.println("=======================================");
        System.exit(0);
    }


}
