# query-engine

多数据源统一查询引擎

这是一款多数据源统一查询引擎工具，暂时可支持 MySQL、Elasticsearch、ClickHouse（待实现）、Phoenix（待实现）等支持 SQL 引擎的数据库。接入方只需自动或手动方式输入数据源 Schema，自动生成类 DSL
语句，即刻可查询，语法简单易懂。当前可支持常规 SQL 分页查询、筛选过滤和 SQL 聚合（group by）等函数，后续可支持 Join、多数据源 Join 联查功能，为繁重的后端工作减轻部分工作量。

- 简单 DSL 语法。
- 支持 SpringBoot 快速接入。
- 查询功能封装，可快速实现自定义开发。

```
打造“简洁 易用 可视化 低代码”的数据查询工具
```

快速集成
-----------------------------------

- 快速集成文档

  https://github.com/luons/query-engine/wiki

- 引入query-engine依赖

```
<dependency>
   <groupId>io.github.luons.engine</groupId>
   <artifactId>engine-cube</artifactId>
   <version>1.0.1.RELEASE</version>
 </dependency>
```

文档
-----------------------------------

- 官方文档： https://github.com/luons/query-engine/wiki

背景
-----------------------------------
数据服务 API 和业务中台是互联网公司必备的一项需求，但是怎么样能实现少代码实现多种复杂分页查询、过滤筛选和聚合。但是行业内并没有通用的查询模块，能快速支撑多种数据源的复杂查询，简单是我们的初衷。

- 直接 Maven 引用模块，简化开发
- 可支持快速扩张自定义开发


query-engine已在我们公司使用了较长的时间，能基本满足多项业务需求。

为什么选择 query-engine
-----------------------------------
> 操作使用简答、快速接入，少量代码可支持后端工作低，这个是你的首选！

数据库兼容
-----------------------------------
支持含常规多种数据库

|  数据库   |  支持   |
| --- | --- |
|   MySQL / MariaDB ....   |  √   |
|   PostgreSQL   |  √   |
|  Elasticsearch   |  √   |
|  ClickHouse   |  可支持   |
|   Phoenix   |  可支持   |
|   HBase   |  可支持   |

功能清单
-----------------------------------





