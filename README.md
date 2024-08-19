# Simple Redis Lock 一个简单的redis分布式锁



这里的简单是指你的项目想要使用该锁简单，你只需要把`RedisLock.java`、`RedisLockConfig.java`、`RedisOperator.java`、`RedisOperatorByStringTemplate.java`这四个文件放到你项目的同一个包下，并给它们添加包名就行了。



其中`RedisLock.java`是锁的核心实现，它需要一个`RedisOperator.java`接口的实现作为redis的数据源。

我这里使用spring-data-redis的StringRedisTemplate做了一个RedisOperator的实现实例，并在RedisLockConfig文件里配置了它们所需要的bean(由于默认使用的 springboot 我没有添加StringRedisTemplate的bean定义，如果你不是springboot 想要使用StringRedisTemplate请自行定义它)。



这个分布式锁使用set nx ex 作为锁的基本功能，并在value处使用 `(基于jvm+thread的uuid)_重入次数`作为value。在你不指定自动释放时间时，会使用RedisLock类中定义的 defaultAutoReleaseTime (你可以任意修改它)作为默认释放时间，并发布一个看门狗每隔该时间的1/3来自动续约。



代码里面关于redis的操作主要是对key本身、string、发布订阅的，没有包含redis其他的数据结构。