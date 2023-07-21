--********************************************************************--
-- Author:         Write your name here
-- Created Time:   2023-07-20 18:01:56
-- Description:    Write your description here
-- Hints:          You can use SET statements to modify the configuration
--********************************************************************--

CREATE TEMPORARY TABLE orders_dataset (
	order_id BIGINT,
  `user_id` bigint,
	auction_id bigint,
	cat_id bigint,
	cat1 bigint,
	property varchar,
	buy_mount int,
	`day` varchar	,
   PRIMARY KEY(order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql',
    'hostname' = '***',
    'port' = '3306',
    'username' = '***',
    'password' = '***',
    'database-name' = '***',
    'table-name' = 'orders_dataset'
);
CREATE TEMPORARY TABLE baby_dataset (
	`user_id` bigint,
	birthday varchar,
	gender int,
    PRIMARY KEY(user_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql',
    'hostname' = '***',
    'port' = '3306',
    'username' = '***',
    'password' = '***',
    'database-name' = '***',
    'table-name' = 'baby_dataset'
);
CREATE TEMPORARY TABLE es_sink(
  day_year varchar,
  `buy_num` bigint,
	baby_num bigint,
  PRIMARY KEY(day_year) NOT ENFORCED  -- 主键可选，如果定义了主键，则作为文档ID，否则文档ID将为随机值。
) WITH (
'connector' = 'elasticsearch-7',
  'hosts' = 'http://***:9200',
  'index' = 'enriched_orders_view',
  'username' ='elastic',
  'password' ='***' --创建ES实例时自定义的密码
);
INSERT INTO es_sink
SELECT
	SUBSTRING(tmp1.`day` FROM 1 FOR 6) as day_year,
	SUM(tmp1.buy_mount) as buy_num,
	COUNT(birthday) as baby_num
FROM(
	SELECT o.*,
		b.birthday,
		b.gender
	FROM orders_dataset /*+ OPTIONS('server-id'='123456-123457') */ o
	LEFT JOIN baby_dataset /*+ OPTIONS('server-id'='123458-123459') */ as b
		ON o.user_id = b.user_id
) tmp1
GROUP BY SUBSTRING(tmp1.`day` FROM 1 FOR 6)
