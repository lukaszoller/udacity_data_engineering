CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer',
  `timestamp` bigint COMMENT 'from deserializer',
  `x` int COMMENT 'from deserializer',
  `y` int COMMENT 'from deserializer',
  `z` int COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'TableType'='EXTERNAL_TABLE')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://borti-stedi/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json')
