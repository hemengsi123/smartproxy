# Smart Proxy For Redis Cluster
![Redis Logo](http://redis.io/images/redis-white.png)

## 简介

随着 Redis Cluster 3.0 发布，公司的 Redis 存储集群基本全部升级到 Cluster模式，做缓存用的仍然使用 Twemproxy 并且开启 auto-eject-host模式。这个过程遇到很多问题，大家可以参考我之前的博客[7月，redis迷情](http://www.jianshu.com/p/9fdb1aece269)，在 Cluster 上层添加 Proxy有如下好处：

1. 隔离线上与后端 Redis Cluster，保护集群
2. 在 Proxy 层做各种访问控制和性能统计，服务注册发现
3. 减少集群模式中客户端的 Move&Ask 操作
4. 有些语言的 Smart Client 实现不健壮，隔离差异


