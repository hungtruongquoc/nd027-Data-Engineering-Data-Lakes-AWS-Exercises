CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://udacity-stedi/step_trainer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
