# i2i_recall

利用hive sql + UDF实现的基于物品邻域的I2I计算框架，可以在HiveOnMr和HiveOnSpark引擎上执行。

实现的算法包括item-CF、Adamic、item-CF-IUF、Swing、LLR，并在单机hive上在公开数据集合上进行了测评。

# docker-hive环境+数据准备

预先装好docker和docker-compose，有hive环境的忽略

准备环境

```
git submodule init && git submodule update 
cd docker-hive
docker-compose up -d # 启动hive整套服务
docker-compose exec hive-server /bin/bash # 进入到hive-server，可以开启hive客户端，执行hive sql了
```

准备数据

数据上选用了MovieLens 1M Dataset，主要是因为单机跑，数据量小一点，下载链接为https://grouplens.org/datasets/movielens/1m/，下载解压缩后目录为ml-1m

```
docker cp ml-1m/ratings.dat `docker ps | grep hive-server | awk '{print $1}'`:/opt # 把下载的数据拷贝到docker容器内
```

在hive内加载数据

```
CREATE TABLE if not exists ml_1m_ratings (text STRING);

LOAD DATA LOCAL INPATH '/opt/ml-1m/ratings.dat' OVERWRITE INTO TABLE ml_1m_ratings;

```

执行ml_1m_data_prepare.sql里面的sql，拷贝到hive客户端里。

# 执行算法

打包hive-udf并拷贝到docker内。

```
cd i2i-udf
hive_server_did=`docker ps | grep hive-server | awk '{print $1}'`
mvn clean && mvn compile && mvn package && docker cp target/hive-0.0.1-SNAPSHOT.jar ${hive_server_did}:/opt/hive-0.0.2-SNAPSHOT.jar
```

每一个算法目录下，都有一个run.sql，可以直接贴在hive里执行。

# 简单效果测评

|算法|ml-1m hr@20|ml-1m hr@100|
|---|---|---|
|item-CF|0.07830610568586292|0.26738044172675|
|adamic|0.0721456603084786|0.24144952998083416|
|item-CF-IUF|0.07868257734781418|0.2682931003011773|
|Swing|0.07369717988500502|0.2525668522405768|
|Swing(div uv)|0.0786027197225518|0.27269667792278907|
|LLR|0.0765777585105412|0.26589737154330567|



