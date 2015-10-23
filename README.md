# Smart Proxy For Redis Cluster
![Redis Logo](http://redis.io/images/redis-white.png)

## 背景

随着 Redis Cluster 3.0 发布，公司的 Redis 存储集群基本全部升级到 Cluster模式，做缓存用的仍然使用 Twemproxy 并且开启 auto-eject-host模式。这个过程遇到很多问题，大家可以参考我之前的博客[7月，redis迷情](http://www.jianshu.com/p/9fdb1aece269)，在 Cluster 上层添加 Proxy有如下好处：

1. 隔离线上与后端 Redis Cluster，保护集群
2. 在 Proxy 层做各种访问控制和性能统计，服务注册发现(当前没有实现)
3. 屏蔽集群模式中客户端的 Move&Ask 操作，Client使用和单机一样
4. 有些语言的 Smart Client 实现不健壮，隔离差异

## 实现

Proxy 实现比较简单，复用 Go Redis Driver，封装个网络层。对 Client 端的输入进行检查(是否禁掉，参数长度是否符合要求)，提取命令，利用 Go Reflect 去执行。代码比较简单，有些实现比较 Ugly，水平有限，大家莫怪。

## 限制

管理命令，危险命令，跨slot命令，聚合命令禁掉：RENAME, RENAMENX, MSETNX, RPOPLPUSH, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SMOVE, ZUNIONSTORE, ZINTERSTORE, BGREWRITEAOF, BGSAVE, BITOP, BLPOP, BRPOP, BRPOPLPUSH, CLIENT, CONFIG, DBSIZE, DEBUG, DISCARD, EXEC, FLUSHALL, FLUSHDB, KEYS, LASTSAVE, MONITOR, MOVE, MSETNX, MULTI, OBJECT, PSUBSCRIBE, PUBLISH, PUNSUBSCRIBE, RANDOMKEY, RENAME, RENAMENX, SAVE, SCAN, SSCAN, HSCAN, ZSCAN, SCRIPT, SHUTDOWN, SLAVEOF, SLOWLOG, SORT, SUBSCRIBE, SYNC, SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SMOVE, SUNION, SUNIONSTORE, TIME, UNSUBSCRIBE, UNWATCH, WATCH, ZUNIONSTORE, ZINTERSTORE

代理层合并的命令：MSET,MGET,DEL. 这些命令将参数打散并行执行，性能较差。根据 Cluster 原理，将 key crc32 值相同的可以在同一个 node 执行，不过当前没有采用。

大家如果有想用的命令，或是实现不对的，随时开 Issue

## 安装
Proxy 无状态，所以一般要部署多个

```
# go get github.com/dongzerun/smartproxy

```

```
# make

```

修改 example.ini 配置，将 nodes 改成自已集群的实例，port 改成自定义端口

```
# ./cmd/redis_proxy  -config_file=example.ini

```


## 测试

两种数据做对比，压测单实例 Redis 和压测单 Proxy + 6 nodes 集群，测试不是为了对比，数据大家看看就好。

```
redis-benchmark -h localhost -p 8889  -n 1000000 -c 100   -r 20 -q 

```

单机单实例 Redis(benchmark与server同一台机器)

```
PING_INLINE: 83097.88 requests per second
PING_BULK: 70967.29 requests per second
SET: 42784.41 requests per second
GET: 54171.18 requests per second
INCR: 82569.56 requests per second
LPUSH: 82501.45 requests per second
LPOP: 82898.12 requests per second
SADD: 82884.38 requests per second
SPOP: 61376.05 requests per second
LPUSH (needed to benchmark LRANGE): 40165.48 requests per second
LRANGE_100 (first 100 elements): 40632.24 requests per second
LRANGE_300 (first 300 elements): 18212.61 requests per second
LRANGE_500 (first 450 elements): 12660.63 requests per second
LRANGE_600 (first 600 elements): 10805.44 requests per second
MSET (10 keys): 34170.51 requests per second
```

Proxy + 6 nodes 集群(proxy与集群在不同机器)

```
PING_INLINE: 83605.05 requests per second
PING_BULK: 83745.08 requests per second
SET: 74928.82 requests per second
GET: 76353.36 requests per second
INCR: 79170.30 requests per second
LPUSH: 79732.10 requests per second
LPOP: 77887.69 requests per second
SADD: 78326.94 requests per second
SPOP: 79402.89 requests per second
LPUSH (needed to benchmark LRANGE): 77784.70 requests per second
LRANGE_100 (first 100 elements): 33512.06 requests per second
LRANGE_300 (first 300 elements): 14437.10 requests per second
LRANGE_500 (first 450 elements): 10643.28 requests per second
LRANGE_600 (first 600 elements): 8321.96 requests per second
MSET (10 keys): 28713.36 requests per second
```

## 改进

由于当前实现比较粗糙，对所有输入的参数做 Byte To String 转化，性能开销和GC压力比较大。后续会从底层完全重写。