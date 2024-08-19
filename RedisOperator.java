import java.util.function.Consumer;

/**
 * Redis锁的 redis 数据源接口，根据不同客户端提供该接口的不同实现并通过构造方法传给RedisLock
 * 其中大部分方法会带有一个 value当前的操作值 和 expectValue预期当前redis里的值（类似于CAS操作）并返回是否操作成功，这个不使用expectValue并直接返回True也能使用（并且大部分场景已经够用了）
 * 当然你也可以使用lua脚本实现更健壮的 RedisOperator
 */
public interface RedisOperator {

    void expire(String key, long releaseMillis);

    Long getExpire(String key);

    Boolean set(String key, String value, String expectValue, long releaseMillis);

    Boolean setNXEX(String key, String value, long releaseMillis);

    String get(String key);

    Boolean del(String key, String expectValue);

    /**
     * 同步的订阅等待锁释放，当收到锁释放消息或超时时返回，并通过 releaseSignal 返回锁是否释放
     * @param channel
     * @param releaseSignal
     * @param timeoutMillis
     */
    void waitReleaseAsync(String channel, Consumer<Boolean> releaseSignal, long timeoutMillis);

    /**
     * 发布锁释放消息
     * @param channel
     */
    void publishRelease(String channel);

}
