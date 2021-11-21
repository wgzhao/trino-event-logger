# trino-event-logger

基于 [Trino](https://trino.io) [Event listener](https://trino.io/docs/current/develop/event-listener.html) 接口开发的查询日志保存插件。

默认情况下， Trino 重启后，之前的查询记录都会丢失，为了保存所有查询历史，便于后续统计分析以及其他用途，开发了这个插件。

## 如何使用

### 获取代码

 ```shell
 git clone https://github.com/wgzhao/trino-event-logger
 cd trino-event-logger
 ```

### 编译

```shell
mvn clean package assembly:assembly
```

### 部署

执行下面的命令，将编译好的 jar 文件拷贝到 trino 集群中（仅需要部署在 coordinator 节点上）

 ```shell
 mkdir /usr/lib/trino/plugin/event-logger/
 cp target/trino-event-logger-*-jar-with-dependencies.jar /usr/lib/trino/plugin/event-logger/
 ```

### 配置

在 coordinator 节点上的执行下面的命令，将会在配置目录下(`/etc/trino`) 创建一个名为 `event-logger.properties` 的文件

 ```shell
 trino config --plugin-config event-logger.properties
 ```

编辑该文件，配置以下内容

 ```ini
 event-listener.name=query-event-logger
 log-dir=/var/log/trino
 log-file=query.log
 separator=|
 header=true
 max-file-size=1000000
 ```

### 重启 coordinator 节点

 ```shell
/etc/init.d/trino restart
 ```

## 测试

连接到 `trino` 集群，任意执行一些 SQL 语句，比如

```sql
select *
from system.runtime.nodes;
```

然后查看 `/var/log/trino/query.log` 文件，可以看到查询日志记录了。

```csv
query_id,query_state,query_user,query_source,query_sql,query_start,query_end,wall_time,queue_time,cpu_time,peak_memory_bytes,query_error_type,query_error_code
20211121_062554_00000_6mf7k,FINISHED,wgzhao,trino-cli,select * from system.runtime.nodes,2021-11-21 14:25:55,2021-11-21 14:25:55,0,0,0,0,,
```

## 配置说明

| 配置项    | 默认值    | 说明            |
|-----------|----------|----------------|
| log-dir   | `/var/log/trino` | 日志目录，该目录必须存在，且有可写权限    |
| log-file  | `query.log`    | 日志文件名   |
| separator | `|`            | 字段分隔符         |
| header    | `true`         | 是否输出表头 |
| max-file-size | `104857600`    | 日志文件最大大小(字节)，超过该大小，则做日志轮转(rotate) |

输出的日志内容以及表头含义如下：

| 表头  | 示例   | 说明    |
|--------|--------|-------------|
| query_id | `20211121_062554_00000_6mf7k` | 查询 ID |
| query_state | `FINISHED` | 查询状态 |
| query_user | `wgzhao` | 查询用户 |
| query_source | `trino-cli` | 查询来源 |
| query_sql | `select * from system.runtime.nodes` | 查询 SQL |
| query_start | `2021-11-21 14:25:55` | 查询开始时间 |
| query_end | `2021-11-21 14:25:55` | 查询结束时间 |
| wall_time | `0` | 查询执行时间(秒) |
| queue_time | `0` | 查询排队时间(秒) |
| cpu_time | `12` | 消耗的 CPU 时间 |
| peak_memory_bytes | `1224` | 消耗的内存峰值 |
| query_error_type | ``  | 查询失败错误类型  |
| query_error_code | `1334` | 查询失败错误码 |

