import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一个简单的带自动续约和可重入功能的redis分布式锁
 */
public class RedisLock {

    private static final String JVM_PID;

    static {
        JVM_PID = _jvmPid();
    }

    private static final String KEY_PREFIX = "dislock:";

    private static final String RELEASE_SIGNAL_PREFIX = "waitLockRelease:";

    // 这个默认自动释放时间指的是服务挂了的释放时间,如果服务没挂会有看门狗自动续期
    private static final long defaultAutoReleaseTime = 10 * 1000;

    private final ScheduledExecutorService watchDogMonitorPool;

    private final Map<String, WatchDogTaskContext> watchDogMonitorMap = new ConcurrentHashMap<>();

    private final RedisOperator redisOperator;

    public RedisLock(RedisOperator redisOperator) {
        this.redisOperator = redisOperator;
        watchDogMonitorPool = new ScheduledThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors() * 2
        );
    }

    /**
     * 获取当前jvm进程唯一标识
     *
     * @return
     */
    public static String _jvmPid() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(localHost);
            // 当前进程 MAC 地址
            byte[] macAddress = network.getHardwareAddress();
            // 当前进程的 PID
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            // MAC 地址和 PID 组合生成 UUID
            UUID uuid = UUID.nameUUIDFromBytes((new String(macAddress) + pid).getBytes());
            return uuid.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 开启看门狗续约
     * @param key
     * @param autoRenewalTime
     * @param lockMark
     * @param isCreate
     */
    private void submitWatchDogMonitor(final String key, long autoRenewalTime, final String lockMark, final boolean isCreate) {
        watchDogMonitorMap.compute(key, (k, v) -> {
            if (stringIsBlank(lockMark)) {
                return v;
            }
            if (v == null && isCreate) {
                v = new WatchDogTaskContext();
                v.setLockMark(lockMark);
                v.setAutoReleaseTime(autoRenewalTime);
            }
            if (v != null && lockMark.equals(v.getLockMark())) {
                v.setScheduledFuture(
                        watchDogMonitorPool.schedule(() -> {
                            String lockValue = redisOperator.get(k);
                            if (stringIsBlank(lockValue) || !lockMark.equals(parseLockMark(lockValue))) {
                                return;
                            }
                            redisOperator.expire(key, autoRenewalTime);
                            submitWatchDogMonitor(key, autoRenewalTime, lockMark, false);
                        }, autoRenewalTime / 3, TimeUnit.MILLISECONDS)
                );
            }
            return v;
        });
    }

    /**
     * 取消看门狗续约
     * @param key
     * @param lockMark
     */
    private void stopWatchDogMonitor(final String key, final String lockMark) {
        watchDogMonitorMap.compute(key, (k, v) -> {
            if (v != null && !stringIsBlank(v.getLockMark()) && v.getLockMark().equals(lockMark)) {
                v.setLockMark(null);
                return null;
            }
            return v;
        });
    }

    /**
     * 从redis的value中解析的当前jvm进程内当前线程的唯一标识
     * @param lockValue
     * @return
     */
    private static String parseLockMark(String lockValue) {
        if (stringIsBlank(lockValue)) {
            throw new IllegalArgumentException("lockValue is empty");
        }
        int lastComma = -1;
        for (int i = lockValue.length() - 1; i >= 0; i--) {
            if (lockValue.charAt(i) == '_') {
                lastComma = i;
                break;
            }
        }
        if (lastComma < 0) {
            throw new IllegalArgumentException("lockValue can't find comma");
        }
        return lockValue.substring(0, lastComma + 1);
    }

    /**
     * 当前锁重入次数
     * @param lockValue
     * @param lockMark
     * @return
     */
    private static int parseReEntrantNum(String lockValue, String lockMark) {
        if (stringIsBlank(lockValue)) {
            throw new IllegalArgumentException("lockValue is empty");
        }
        return Integer.parseInt(lockValue.substring(lockMark.length()));
    }

    private static String getLockKey(String keyPrefix) {
        return KEY_PREFIX + keyPrefix;
    }

    private static String getLockValuePrefix() {
        return JVM_PID + ":" + Thread.currentThread().getId() + "_";
    }

    private static final int SUCCESS = -2;

    private static final int EXCEPTION = -1;

    /**
     * 内部获取一次锁的方法
     * @param key redisKey
     * @param releaseTime 自动释放时间，小于等于0时开启看门狗自动续约
     * @return 获取成功时返回 -2 ，异常返回 -1，其他情况返回锁的剩余时间
     */
    private long _tryLockOnce(String key, long releaseTime) {
        try {
            Long expire;
            String lockValuePrefix = getLockValuePrefix();
            boolean specifiedReleaseTime = releaseTime > 0;
            releaseTime = specifiedReleaseTime ? releaseTime : defaultAutoReleaseTime;
            while (true) {
                Boolean absented = redisOperator.setNXEX(key, lockValuePrefix + "1", releaseTime);

                // 获取锁成功
                if (Boolean.TRUE.equals(absented)) {
                    if (!specifiedReleaseTime) {
                        // 触发看门狗
                        submitWatchDogMonitor(key, releaseTime, lockValuePrefix, true);
                    }
                    return SUCCESS;
                }

                long startPeekExpireTime = System.currentTimeMillis();
                expire = redisOperator.getExpire(key);
                String lockValue = redisOperator.get(key);
                if (expire == null || expire == 0 || stringIsBlank(lockValue)) {
                    continue;
                }
                String curLockValuePrefix = parseLockMark(lockValue);
                int reEntrantNum = parseReEntrantNum(lockValue, curLockValuePrefix);

                // 锁到期了
                long endPeekExpireTime = System.currentTimeMillis();
                if (endPeekExpireTime - startPeekExpireTime >= expire) {
                    continue;
                }

                // 锁重入
                if (curLockValuePrefix.equals(lockValuePrefix)) {
                    if (Boolean.TRUE.equals(redisOperator.set(key, lockValuePrefix + (reEntrantNum + 1), curLockValuePrefix, releaseTime))) {
                        return SUCCESS;
                    } else {
                        return EXCEPTION;
                    }
                }
                break;
            }
            return expire;
        } catch (Exception e) {
            return EXCEPTION;
        }
    }

    public boolean lock(String uniqueValue) {
        return tryLock(uniqueValue, -1, -1, null);
    }

    public boolean tryLock(String uniqueValue, long timeout, TimeUnit unit) {
        return tryLock(uniqueValue, timeout, -1, unit);
    }

    /**
     * 获取可重入分布式锁
     * @param uniqueValue 锁的唯一key，与通用前缀组成redis key
     * @param timeout 等待超时时间超时
     * @param autoReleaseTime 自动释放时间
     * @param timeUnit 等待单位
     * @return 返回是否获取成功
     */
    public boolean tryLock(String uniqueValue, long timeout, long autoReleaseTime, TimeUnit timeUnit) {
        try {
            long startTime = System.currentTimeMillis();
            String key = getLockKey(uniqueValue);
            timeout = timeout > 0 ? timeUnit.toMillis(timeout) : Long.MAX_VALUE;
            autoReleaseTime = autoReleaseTime > 0 ? timeUnit.toMillis(autoReleaseTime) : -1;
            long expireTime;
            while ((expireTime = _tryLockOnce(key, autoReleaseTime)) != SUCCESS) {
                if (expireTime <= 0) {
                    expireTime = Integer.MAX_VALUE;
                }
                timeout -= System.currentTimeMillis() - startTime;
                if (timeout <= 0) {
                    return false;
                }
                AtomicBoolean isReleased = new AtomicBoolean(false);

                // 阻塞等一会（两种退出情况：1.设置的超时时间到了 2.锁释放了）
                redisOperator.waitReleaseAsync(RELEASE_SIGNAL_PREFIX + key, isReleased::set, Math.min(expireTime, timeout));
                if (!Boolean.TRUE.equals(isReleased.get()) && timeout <= expireTime) {
                    return false;
                }
                startTime = System.currentTimeMillis();
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 解锁，如果开启了看门狗会自动关闭它
     * @param uniqueValue
     */
    public void unlock(String uniqueValue) {
        String key = getLockKey(uniqueValue);
        String lockValuePrefix = getLockValuePrefix();
        String lockValue = redisOperator.get(key);
        if (stringIsBlank(lockValue)) {
            return;
        }
        String curLockValuePrefix = parseLockMark(lockValue);
        int reEntrantNum = parseReEntrantNum(lockValue, curLockValuePrefix);

        if (!stringIsBlank(curLockValuePrefix) && curLockValuePrefix.equals(lockValuePrefix)) {
            if (--reEntrantNum > 0) {
                WatchDogTaskContext watchDogTaskContext = watchDogMonitorMap.get(key);
                if (watchDogTaskContext != null) {
                    redisOperator.set(key, lockValuePrefix + reEntrantNum, lockValue, watchDogTaskContext.getAutoReleaseTime());
                }
            } else {
                stopWatchDogMonitor(key, lockValuePrefix);
                if (Boolean.TRUE.equals(redisOperator.del(key, lockValue))) {
                    redisOperator.publishRelease(RELEASE_SIGNAL_PREFIX + key);
                }
            }
        }
    }

    static class WatchDogTaskContext {

        private volatile ScheduledFuture<?> scheduledFuture;

        private volatile long autoReleaseTime;

        private volatile String lockMark;

        public String getLockMark() {
            return lockMark;
        }

        public void setLockMark(String lockMark) {
            this.lockMark = lockMark;
        }

        public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
            this.scheduledFuture = scheduledFuture;
        }

        public void setAutoReleaseTime(long autoReleaseTime) {
            this.autoReleaseTime = autoReleaseTime;
        }

        public ScheduledFuture<?> getScheduledFuture() {
            return scheduledFuture;
        }

        public long getAutoReleaseTime() {
            return autoReleaseTime;
        }

        @Override
        public String toString() {
            return "WatchDogTaskContext{" +
                    "scheduledFuture=" + scheduledFuture +
                    ", autoReleaseTime=" + autoReleaseTime +
                    ", lockMark='" + lockMark + '\'' +
                    '}';
        }
    }

    /**
     * 判断字符串是否为空
     * @param cs
     * @return true代表是空字符串(去除空格)或null
     */
    private static boolean stringIsBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

}
