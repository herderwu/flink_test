--********************************************************************--
-- Author:         Write your name here
-- Created Time:   2023-07-20 11:30:28
-- Description:    Write your description here
-- Hints:          You can use SET statements to modify the configuration
--********************************************************************--


-- 通过DDL语句创建SLS源表，SLS中存放了Github的实时数据。
CREATE TEMPORARY TABLE gh_event(
  id STRING,                                        -- 每个事件的唯一ID。
  created_at BIGINT,                                -- 事件时间，单位秒。
  created_at_ts as TO_TIMESTAMP(created_at*1000),   -- 事件时间戳（当前会话时区下的时间戳，如：Asia/Shanghai）。
  type STRING,                                      -- Github事件类型，如：。ForkEvent, WatchEvent, IssuesEvent, CommitCommentEvent等。
  actor_id STRING,                                  -- Github用户ID。
  actor_login STRING,                               -- Github用户名。
  repo_id STRING,                                   -- Github仓库ID。
  repo_name STRING,                                 -- Github仓库名，如：apache/flink, apache/spark, alibaba/fastjson等。
  org STRING,                                       -- Github组织ID。
  org_login STRING                                 -- Github组织名，如： apache,google,alibaba等。
) WITH (
  'connector' = 'sls',                              -- 实时采集的Github事件存放在阿里云SLS中。
  'project' = '***',                     -- 存放公开数据的SLS项目。例如'***'。
  'endPoint' = '***',                   -- 公开数据仅限VVP通过私网地址访问。例如'***'。
  'logStore' = 'realtime-github-events',            -- 存放公开数据的SLS logStore。
  'accessId' =  '***',         -- 只读账号的AK。
  'accessKey' = '***',   -- 只读账号的SK。
  'batchGetSize' = '500',                           -- 批量读取数据，每批最多拉取500条。
  'startTime' = '2023-06-30 14:00:00'              -- 开始时间，尽量设置到需要计算的时间附近，否则无效计算的时间较长。默认值为当前值
);

-- 配置开启mini-batch, 每2s处理一次。
SET 'table.exec.mini-batch.enabled'='true';
SET 'table.exec.mini-batch.allow-latency'='2s';
SET 'table.exec.mini-batch.size'='4096';

-- 作业设置4个并发，聚合更快。
SET 'parallelism.default' = '4';


-- 查看Github新增star数Top 5仓库。
SELECT DATE_FORMAT(created_at_ts, 'yyyy-MM-dd') as `date`, repo_name, COUNT(*) as num
FROM gh_event WHERE type = 'WatchEvent'
GROUP BY DATE_FORMAT(created_at_ts, 'yyyy-MM-dd'), repo_name
ORDER BY num DESC
LIMIT 5;
