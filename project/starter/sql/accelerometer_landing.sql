CREATE EXTERNAL TABLE user_movement (
    user STRING,
    timestamp BIGINT,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://udacity-stedi/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
