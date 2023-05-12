package com.work.order.distributeLock;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.json.JSONUtil;
import lombok.AllArgsConstructor;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.spring.data.connection.RedissonConnectionFactory;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;


/**
 * @program: central-platform
 * @description: com.rocketmq.demo.ConcurrentHashMapLock.SegmentDistributeLock.java
 * @Author admin
 * @Date 2021/7/6 12:00
 */
//@Slf4j
public class SegmentDistributeLock {
    /**
     *  使用redis分布式锁扣减库存,弊端: 请求量大的话,会导致吞吐量降低
     *  优化: 分段锁并发扣减库存
     *      将表中的库存字段 分为 5个库存字段, 然后导入redis,库存预热, 然后参考ConcurrentHashMap的分段锁思想
     *      来一个请求后,对库存字段 加 分段锁, 分段锁扣减库存
     *      如果当前分段锁库存不够,就扣减掉当前的库存,然后去锁下一个分段锁,扣减库存
     *
     *      git: https://gitee.com/easybao/segmentDistributeLock.git
     *      依赖jar包:
     *       <dependency>
     *             <groupId>org.redisson</groupId>
     *             <artifactId>redisson</artifactId>
     *             <version>3.13.5</version>
     *         </dependency>
     */
    static final RedissonClient redissonClient;
    RBucket<RedisStock[]> bucket;
    private ThreadLocal<StockRequest> threadLocal = new ThreadLocal<>();
    static volatile RedisStock[] redisStocks;

    private static StringRedisTemplate redisTemplate;

    private static int DEFAULT_CONCURRENCY_LEVEL = 1; //默认并发数
    private static final float LOAD_FACTOR = 0.75f;
    private static long DEFAULT_STOCK_CAPACITY = 0; //初始总库存,避免并发过程中 调用getStockCapacity()获取到的总库存发生变化
    RedisAtomicLong stockCounter;
    RedisAtomicLong taskCounter;



    public SegmentDistributeLock() {
        this(DEFAULT_STOCK_CAPACITY);
    }

    public SegmentDistributeLock(long stockCapacity)  {
        this(stockCapacity, DEFAULT_CONCURRENCY_LEVEL);
    }

    public SegmentDistributeLock(long stockCapacity, int concurrencyLevel) {
        if (stockCapacity < 0 || concurrencyLevel <= 0)
            throw new IllegalArgumentException();
        this.DEFAULT_STOCK_CAPACITY = stockCapacity;
        this.DEFAULT_CONCURRENCY_LEVEL = concurrencyLevel;
        init();
    }

    public void init() {
        long segmentSize = this.partValue(DEFAULT_STOCK_CAPACITY, DEFAULT_CONCURRENCY_LEVEL);
        long segmentMod = DEFAULT_STOCK_CAPACITY % DEFAULT_CONCURRENCY_LEVEL;
        redisStocks = new RedisStock[DEFAULT_CONCURRENCY_LEVEL];
        for (int i = 0; i < DEFAULT_CONCURRENCY_LEVEL; i++) {
            long segmentStock = segmentMod==0 || i < segmentMod ? segmentSize : segmentSize - 1;
            redisStocks[i] = new RedisStock(String.format("applets:order:pId_stock_%05d", i+1), segmentStock);
        }
        // 初始总库存
        this.DEFAULT_STOCK_CAPACITY = getCurrentTotalCapacity();
        stockCounter = new RedisAtomicLong("applets:order:lastest_stocapacity", redisTemplate.getConnectionFactory(), DEFAULT_STOCK_CAPACITY);

        // 库存预热,存到redis中 ,  这里没有采用因为将库存预热存到redis中,取出来的时候,解析异常, 不想花时间解决,所以将库存预热 变成一个类变量
//        bucket = redissonClient.getBucket("pId_stock");
//        bucket.set(redisStocks);
        taskCounter= new RedisAtomicLong("applets:order:task_counter", redisTemplate.getConnectionFactory(), 0);
    }


    /**
     *  使用redis分布式锁扣减库存,弊端: 请求量大的话,会导致吞吐量降低
     *  优化: 分段锁并发扣减库存
     *      将表中的库存字段 分为 5个库存字段, 然后导入redis,库存预热, 然后参考ConcurrentHashMap的分段锁思想
     *      来一个请求后,对库存字段 加 分段锁, 分段锁扣减库存
     *      如果当前分段锁库存不够,就扣减掉当前的库存,然后去锁下一个分段锁,扣减库存
     * @param request
     * @return
     */
    public boolean handlerStock(StockRequest request) {
        // 先做校验: 判断扣减库存 是否比 初始总库存还大,是的话就直接false,  避免无限循环扣减不了
        if(request.getBuyNum() <= 0 || request.getBuyNum() > stockCounter.get() || stockCounter.get() <= 0){
            return false;
        }
        String numbers = RandomUtil.randomNumbers(6);
        // 使用本地线程变量保存请求,确保参数只在本线程使用
        threadLocal.set(request);

        // 这里使用 ThreadLocal代码逻辑和ConcurrentHashMap的分段锁
        RedissonClient redissonClient = getRedissonClient();
        RedisStock[] tab = redisStocks;
        int len = tab.length;
        int lockIndex = Math.abs(request.getMemberId().hashCode()) % len, i = lockIndex;
        //环形平均算法
        for(RedisStock e = tab[lockIndex]; e != null && i < lockIndex + len; e = tab[nextIndex(i++, len)]){
            RLock segmentLock = null;
            try {
                //购买数量扣除多个分片锁判断
                if (threadLocal.get().getBuyNum() <= 0) {
                    System.out.println("1111111111111111111111111111111111111111111111111111111111111111111");
                    break;
                }

                // 2: 对该元素加分布式分段锁
                segmentLock = redissonClient.getLock(e.getStockName());
                segmentLock.lock();
                //业务逻辑

                long buyNum = threadLocal.get().getBuyNum();
                if (buyNum <= e.getStockCapacity()) {
                    redisTemplate.opsForHash().put("applets:order:consume_sorder", Long.toString(request.id) + "-" + taskCounter.getAndIncrement(), Long.toString(buyNum));
                    //扣减库存
                    e.setStockCapacity(e.getStockCapacity() - buyNum);
                    stockCounter.addAndGet(-buyNum);
                    //扣减成功后保存到数据库中
                    // 扣减成功后,跳出循环,返回结果
                    return true;
                } else if (e.getStockCapacity() >0) {
                    // 库存数量和购买数量为0时，购买失败返回
                    if (stockCounter.get() <= 0 || buyNum <= 0 )
                        break;

                    // 如果并发过程中获取到总库存<= 0 说明已经没有库存了,  如果当前需要扣减的库存 > 此时总库存就返回false,扣件失败
                    if (buyNum > stockCounter.get()) {
                        // 没有库存就false
                        System.out.println(Thread.currentThread().getName() + " 扣减库存数: " + buyNum + " 失败," + "   此时总库存为: " + stockCounter.get());
                        break;
                    }
                    //总库存满足购买数量，分段锁里数量不够购买，首先把分段锁库存保存，然后把购买数量减去分段锁的库存，把分段锁的库存清零
                    redisTemplate.opsForHash().put("applets:order:consume_sorder", Long.toString(request.id) + "-" + taskCounter.getAndIncrement(), Long.toString(e.getStockCapacity()));
                    //购买数量大于分段库存时，把购买数量拆分多个处理
                    // 扣减掉当前的 分段锁对应的库存,然后对下一个元素加锁
                    threadLocal.get().setBuyNum(buyNum - e.getStockCapacity());
                    stockCounter.addAndGet(-e.getStockCapacity());
                    e.setStockCapacity(0);
                }
            } finally {
                // 3: 解锁
//                segmentLock = redissonClient.getLock(e.getStockName());
//                if (segmentLock != null && segmentLock.isHeldByCurrentThread()) {
                    segmentLock.unlock();
//                }
            }

        }
        //购买数量大于多个库存锁的库存数量是，最后购买数量不足时，需要回滚库存
        redisTemplate.opsForHash().put("applets:order:afail_sorder", Long.toString(request.id) + "-" + numbers, Long.toString(request.getBuyNum()));
        redisTemplate.opsForHash().put("applets:order:fail_sorder", Long.toString(request.id) + "-" + numbers, Long.toString(request.getOriginalNum()));
        threadLocal.remove();
        return false;
    }


    public RedissonClient getRedissonClient(){
        return this.redissonClient;
    }

    public StringRedisTemplate getRedisTemplate() {
        return redisTemplate;
    }

    public static long getCurrentTotalCapacity(){
        // 获取实时总库存
        return Stream.of(redisStocks).mapToLong(RedisStock::getStockCapacity).sum();
    }

    // 显示redis中的库存
    public void showStocks(){
        System.out.println(JSONUtil.toJsonStr(redisStocks));
//        for (RedisStock redisStock : redisStocks) {
//            System.out.println(JSONUtil.toJsonStr(redisStock));
//        }
    }

    /**
     * 基于环形平均算法
     *
     * @param index
     * @param len
     * @return
     */
    private static int nextIndex(int index, int len) {
//        return ((index + 1 < len) ? index + 1 % len : 0);
        return ((index + 1) % len < len ? (index + 1) % len : 0);
    }

    /**
     * 平均分配数值
     * @param total
     * @param partCount
     * @return
     */
    private static long partValue(long total, int partCount) {
        long partValue = total / partCount;
        if (total % partCount > 0) {
            ++partValue;
        }
        return partValue;
    }

    static {
        Config config = new Config();
//        config.useSingleServer().setAddress("redis://192.168.32.16:6379").setPassword("mtmy");
        config.useSingleServer().setAddress("redis://127.0.0.1:6379").setPassword("Ynby2021")
                .setDatabase(6)//设置数据库位置
                .setConnectionPoolSize(1000)//设置对于master节点的连接池中连接数最大为1000
                .setIdleConnectionTimeout(10000)//如果当前连接池里的连接数量超过了最小空闲连接数，而同时有连接空闲时间超过了该数值，那么这些连接将会自动被关闭，并从连接池里去掉。时间单位是毫秒。
                .setConnectTimeout(30000)//同任何节点建立连接时的等待超时。时间单位是毫秒。
                .setPingConnectionInterval(50000)//此项务必设置为redisson解决之前bug的timeout问题关键。时间单位是毫秒。
                .setTimeout(100000);//等待节点回复命令的时间。该时间从命令发送成功时开始计时。
        redissonClient = Redisson.create(config);

        RedisConnectionFactory redisConnectionFactory = new RedissonConnectionFactory(redissonClient);
        redisTemplate = new StringRedisTemplate();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        StringRedisSerializer serializer = new StringRedisSerializer();
        redisTemplate.setDefaultSerializer(serializer);
        redisTemplate.setKeySerializer(serializer);
        redisTemplate.setValueSerializer(serializer);

        /**必须执行这个函数,初始化RedisTemplate*/
        redisTemplate.afterPropertiesSet();
    }

    @AllArgsConstructor
    class RedisStock implements Serializable {
        // 库存字段
        String stockName;
        // 库存容量数据, 原子类来保证原子性 num的原子性
        AtomicLong stockCapacity;

        public RedisStock(String stockName, long stockCapacity) {
            this.stockName = stockName;
            this.stockCapacity = new AtomicLong(stockCapacity);
        }

        public void setStockCapacity(long stockCapacity) {
            this.stockCapacity.set(stockCapacity);
        }

        public String getStockName() {
            return stockName;
        }

        public void setStockName(String stockName) {
            this.stockName = stockName;
        }

        public long getStockCapacity() {
            return this.stockCapacity.get();
        }

    }



}
