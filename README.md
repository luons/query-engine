# query-engine - 查询引擎
多数据源统一查询引擎

为打造一款多数据源统一查询引擎工具，可支持MySQL、Elasticsearch、Phoenix等支持SQL引擎的数据库。前端只需自动或手动方式，自动生成类DSL语句，即刻可查询，语法简单易懂。当前可支持常规SQL分页查询、筛选过滤、SQL聚合（group by）等函数，后续支持Join，为繁重的后端工作减轻部分工作量。

- Web 版报表设计器，类似于excel操作风格，通过拖拽完成报表设计。
- 秉承“简单、易用、专业”的产品理念，极大的降低报表开发难度、缩短开发周期、节省成本、解决各类报表难题。
- 领先的企业级Web报表软件，采用纯Web在线技术，专注于解决企业报表快速制作难题。

```
打造“简洁 易用 可视化 低代码”的数据查询工具
```

快速体验：


快速集成
-----------------------------------
- 快速集成文档

  http://report.jeecg.com/2078875

- 引入query-engine依赖


```
<dependency>
  <groupId>org.jeecgframework.jimureport</groupId>
  <artifactId>jimureport-spring-boot-starter</artifactId>
  <version>1.3.795</version>
</dependency>
```


- 执行数据库脚本

  [jimureport.sql](https://github.com/zhangdaiscott/JimuReport/blob/master/db "jimureport.sql")

- 免安装运行版

  [Quickstart版本](https://static.jeecg.com/files/jimureport/jimureport-quickstart.zip)


开发文档
-----------------------------------

- 官方网站： http://www.jimureport.com
- 官方文档： http://report.jeecg.com
- 视频教程： http://jimureport.com/doc/video

![输入图片说明](https://jeecgos.oss-cn-beijing.aliyuncs.com/files/jimureport_qq_qun1.png "在这里输入图片标题")


产生背景
-----------------------------------
报表是企业IT服务必备的一项需求，但是行业内并没有一个免费好用的报表，大部分免费的报表功能较弱也不够智能，商业报表又很贵，所以有了研发一套免费报表的初衷。
做一个什么样的报表呢？随着低代码概念的兴起，原先通过报表工具设计模板，再与系统集成的模式已经落伍，现在追求的是完全在线设计，傻瓜式的操作，实现简单易用又智能的报表！

- 目前积木报表已经实现了完全在线设计，轻量级集成、类似excel的风格，像搭建积木一样在线拖拽设计报表！功能涵盖数据报表设计、打印设计、图表设计、大屏设计等！
- 2019年底启动积木报表研发工作，历经一年多的时间，2020-11-03第一版出炉 [v1.0-beta](https://www.oschina.net/news/119666/jimureport-1-0-beta-released)
- 2020年的持续打磨和研发，终于在2021-1-18发布了第一个正式版本 [v1.1.05](https://www.oschina.net/news/126916/jimureport-1-1-05-released)
- 更多版本日志查看 [发布日志](http://jimureport.com/doc/log)


为什么选择 JimuReport?
-----------------------------------
>    永久免费，支持各种复杂报表，并且傻瓜式在线设计，非常的智能，低代码时代，这个是你的首选！

- 采用SpringBoot的脚手架项目，都可以快速集成
- Web 版设计器，类似于excel操作风格，通过拖拽完成报表设计
- 通过SQL、API等方式，将数据源与模板绑定。同时支持表达式，自动计算合计等功能，使计算工作量大大降低
- 开发效率很高，傻瓜式在线报表设计，一分钟设计一个报表，又简单又强大
- 支持 ECharts，目前支持28种图表，在线拖拽设计，支持SQL和API两种数据源
- 支持分组、交叉，合计、表达式等复杂报表
- 支持打印设计（支持套打、背景打印等）可设置打印边距、方向、页眉页脚等参数 一键快速打印 同时可实现发票套打，不动产证等精准、无缝打印
- 大屏设计器支持几十种图表样式，可自由拼接、组合，设计炫酷大屏
- 可设计各种类型的单据、大屏，如出入库单、销售单、财务报表、合同、监控大屏、旅游数据大屏等



数据库兼容
-----------------------------------
支持含常规、国产、大数据等28种数据库

|  数据库   |  支持   |
| --- | --- |
|   MySQL   |  √   |
|  Oracle、Oracle9i   |  √   |
|  SqlServer、SqlServer2012   |  √   |
|   PostgreSQL   |  √   |
|   DB2、Informix   |  √   |
|   MariaDB   |  √   |
|  SQLite、Hsqldb、Derby、H2   |  √   |
|   达梦、人大金仓、神通   |  √   |
|   华为高斯、虚谷、瀚高数据库   |  √   |
|   阿里云PolarDB、PPAS、HerdDB   |  √   |
|  Hive、HBase、CouchBase   |  √   |



报表截图
-----------------------------------



功能清单
-----------------------------------
更多功能清单: [http://jimureport.com/plan](http://jimureport.com/plan)






