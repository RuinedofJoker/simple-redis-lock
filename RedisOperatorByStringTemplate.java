import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 基于spring-data-redis的StringRedisTemplate实现的不使用lua脚本的 RedisOperator
 */
public class RedisOperatorByStringTemplate implements RedisOperator {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisMessageListenerContainer redisContainer;

    private final ScheduledExecutorService unsubscribeExecutor = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 2
    );

    @Override
    public void expire(String key, long releaseMillis) {
        stringRedisTemplate.expire(key, releaseMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public Long getExpire(String key) {
        return stringRedisTemplate.getExpire(key, TimeUnit.MILLISECONDS);
    }

    @Override
    public Boolean set(String key, String value, String expectValue, long releaseMillis) {
        stringRedisTemplate.opsForValue().set(key, value, releaseMillis, TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public Boolean setNXEX(String key, String value, long releaseMillis) {
        return stringRedisTemplate.opsForValue().setIfAbsent(key, value, releaseMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public String get(String key) {
        return stringRedisTemplate.opsForValue().get(key);
    }

    @Override
    public Boolean del(String key, String expectValue) {
        stringRedisTemplate.delete(key);
        return true;
    }

    @Override
    public void waitReleaseAsync(String channel, Consumer<Boolean> releaseSignal, long timeoutMillis) {
        AtomicBoolean useConsumer = new AtomicBoolean(false);
        AtomicReference<ScheduledFuture<?>> future = new AtomicReference<>(null);
        MessageListener messageListener = (message, pattern) -> {
            if (useConsumer.compareAndSet(false, true)) {
                releaseSignal.accept(true);
            }
            if (future.get() != null) {
                future.get().cancel(false);
            }
        };
        ScheduledFuture<?> scheduledFuture = unsubscribeExecutor.schedule(() -> {
            unsubscribe(channel, messageListener);
            if (useConsumer.compareAndSet(false, true)) {
                releaseSignal.accept(false);
            }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
        future.set(scheduledFuture);
        redisContainer.addMessageListener(messageListener, new PatternTopic(channel));

        try {
            scheduledFuture.get();
        } catch (Exception e) {
            if (useConsumer.compareAndSet(false, true)) {
                releaseSignal.accept(false);
            }
        }
    }

    public void unsubscribe(String channel, MessageListener messageListener) {
        redisContainer.removeMessageListener(messageListener, new PatternTopic(channel));
    }

    @Override
    public void publishRelease(String channel) {
        stringRedisTemplate.convertAndSend(channel, "ok");
    }
}
