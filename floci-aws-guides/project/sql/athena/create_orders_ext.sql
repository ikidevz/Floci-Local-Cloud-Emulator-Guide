-- REAL AWS ONLY. On Floci register the table through the Glue API instead (src/glue_ops/catalog.py).
CREATE EXTERNAL TABLE IF NOT EXISTS floci_demo_db.orders_raw (
    order_id    string,
    customer_id string,
    amount      double,
    status      string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://floci-demo-raw/raw/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');
