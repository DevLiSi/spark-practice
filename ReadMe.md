#### 环境准备
##### spark
本地部署Spark集群（1 Master + 2 Worker）
* command

  docker-compose up -d

* 任务提交
  ```
  docker exec -it spark-master /bin/bash
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/xxx.py
   
* datalake

  将本机文件系统当作datalake，没有在本地搭建hadoop，将本地路径挂载到容器内
  /Users/sili/Bigdata/local_data_lake -> /mnt/spark_data_lake

##### airflow
推荐使用pip方式部署启动，[官网教程](https://airflow.apache.org/docs/apache-airflow/stable/start.html，)
```
pip install apache-airflow
pip intall apache-airflow-providers-apache-spark (解决connection没有spark类型的问题)
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email admin@example.com
airflow webserver --port 8090 （8090避免和spark的web ui冲突）
airflow scheduler
```
用官方docker方式启动的容器多内存可能跑不起来，而且没找到方案解决部署后connection里没有spark类型的问题


#### 作业处理
整体作业处理比较简单，通过pyhon的api拉取并解压文件，然后利用spark的api读取csv并进行转换保存,再将保存后的结果load到PG数据库。

因为本地拉取zip文件较慢，尝试使用多线程，因为资源问题开启10个线程后拉取会卡住，于是为了后续流程能进行，只拉取了2个zip文件。


#### 遇到问题
##### 本地
1.提交到本地spark集群，读取aws s3会有权限异常问题，但是本地直接执行脚本可以读取，没找到原因

2.airflow官网docker部署比较吃内存，部署后connection里没有spark类型，官网建议通过pip的方式部署运行

##### aws
1.aws emr创建有点麻烦，需要自己建vpc和子网，然后执行job需要在web界面提交，没有对外暴露的ip，如果想在外部调度不知道该如何操作

2.在aws上跑的时候需要鉴权，这部分信息写死在代码，不能类似本地可以直接export环境变量