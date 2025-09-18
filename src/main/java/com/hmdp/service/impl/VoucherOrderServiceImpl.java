package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final String STREAM_KEY = "stream.orders";
    private static final String GROUP_NAME = "g1";
    private static final String CONSUMER_NAME = "c1";

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        initializeStreamAndGroup();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 初始化Redis Stream和消费者组
     */
    private void initializeStreamAndGroup() {
        try {
            // 尝试创建消费者组
            stringRedisTemplate.opsForStream().createGroup(STREAM_KEY, GROUP_NAME);
            log.info("成功创建Redis Stream消费者组: stream={}, group={}", STREAM_KEY, GROUP_NAME);
        } catch (RedisSystemException e) {
            if (e.getCause() instanceof RedisCommandExecutionException) {
                String errorMsg = e.getCause().getMessage();
                if (errorMsg.contains("NOGROUP")) {
                    // 流不存在，先创建流再创建消费者组
                    Map<String, String> initialMessage = new HashMap<>();
                    initialMessage.put("init", "true");
                    stringRedisTemplate.opsForStream().add(STREAM_KEY, initialMessage);
                    stringRedisTemplate.opsForStream().createGroup(STREAM_KEY, GROUP_NAME);
                    log.info("创建Redis Stream和消费者组: stream={}, group={}", STREAM_KEY, GROUP_NAME);
                } else if (errorMsg.contains("BUSYGROUP")) {
                    // 消费者组已存在，忽略
                    log.info("消费者组已存在: stream={}, group={}", STREAM_KEY, GROUP_NAME);
                }
            } else {
                log.error("初始化Redis Stream和消费者组失败", e);
            }
        }
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())
                    );

                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }

                    // 3.解析数据并处理订单
                    processOrderRecord(list.get(0));
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        /**
         * 处理订单记录
         */
        private void processOrderRecord(MapRecord<String, Object, Object> record) {
            try {
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                // 创建订单
                createVoucherOrder(voucherOrder);

                // 确认消息
                stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, record.getId());
                log.info("成功处理订单: {}", voucherOrder.getId());
            } catch (Exception e) {
                log.error("处理订单记录失败: {}", record, e);
                throw e;
            }
        }

        /**
         * 处理pending-list中的消息
         */
        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_KEY, ReadOffset.from("0"))
                    );

                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有异常消息，结束循环
                        break;
                    }

                    // 3.处理订单记录
                    processOrderRecord(list.get(0));
                } catch (RedisSystemException e) {
                    if (e.getCause() instanceof RedisCommandExecutionException &&
                            e.getCause().getMessage().contains("NOGROUP")) {
                        // 消费者组不存在，重新初始化
                        initializeStreamAndGroup();
                    } else {
                        log.error("处理pending-list异常", e);
                        try {
                            Thread.sleep(200); // 添加延迟防止快速失败循环
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } catch (Exception e) {
                    log.error("处理pending-list异常", e);
                    try {
                        Thread.sleep(200); // 添加延迟防止快速失败循环
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    /**
     * 创建优惠券订单
     */
    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！userId: {}", userId);
            return;
        }

        try {
            // 1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！userId: {}, voucherId: {}", userId, voucherId);
                return;
            }

            // 3.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！voucherId: {}", voucherId);
                return;
            }

            // 4.创建订单
            save(voucherOrder);
            log.info("成功创建订单: {}", voucherOrder.getId());
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 3.返回订单id
        return Result.ok(orderId);
    }
}
