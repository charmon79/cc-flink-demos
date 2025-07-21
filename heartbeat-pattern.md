
## Demo

### Generate some test data

Let's use the `examples.marketplace.orders` data generator to generate some mock data for an `orders_test` table.

Let this statement run for several minutes, then stop it. (After all, what we want to demonstrate here is the behavior when we do NOT have new data arriving on the `orders_test` table, so it can't be running if the demo's going to be effective.)

```
CREATE TABLE `orders_test`
AS
SELECT *
FROM examples.marketplace.orders;
```

### Create a heartbeat topic

Create a heartbeat topic which will always have new events being produced to it constantly.

For the sake of demonstration, we'll use the example data generators available in Confluent Cloud's Flink service to constantly emit records. In production, you may prefer to do this with a simple Kafka producer client.

```
CREATE TABLE heartbeat
(
  id int,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR ts AS ts
);
```

```
INSERT INTO heartbeat (id)
SELECT 1 FROM examples.marketplace.orders;
```

### UNION the heartbeats to the "real" table to always push event time forward

By using UNION ALL to concatenate the heartbeat table to the "real" table (`orders_test`, in this example), we can ensure that the overall Flink job continuously has new records coming in which can advance the watermark.

Pay attention to a couple of things here:

1. We have to cast the heartbeat records to NULLs that match the schema of the "real" records we want.
1. We need to filter out the heartbeat records from our results, or else there will be a lot of garbage in the results. (This is easily done here, since we know `order_id` will always be NULL for the heartbeat events.)

**NOTE:** Idleness in the source partitions of the "real" table will still impact when results are emitted. The watermark for the overall job is the earliest of the watermarks of all the inputs. Thus, if no new records are arriving on the `orders_test` table, its watermark will not advance until the [idle timeout](https://www.youtube.com/watch?v=YSIhM5-Sykw) (defined by the `sql.tables.scan.idle-timeout` session property) is reached.

```
CREATE TABLE orders_window_dedupe_with_heartbeat
  AS
WITH orders_cte AS (
  SELECT
    order_id,
    customer_id,
    product_id,
    price,
    `$rowtime` AS ts
  FROM orders_test
  UNION ALL
  SELECT
    CAST(NULL AS string) AS order_id,
    CAST(NULL AS int) AS customer_id,
    CAST(NULL AS string) AS product_id,
    CAST(NULL AS double) AS price,
    ts
  FROM heartbeat
)
SELECT *
FROM (
  SELECT
    order_id,
    customer_id,
    product_id,
    price,
    ROW_NUMBER() OVER (PARTITION BY order_id, window_start, window_end ORDER BY ts DESC) AS rownum,
    ts AS orig_rowtime,
    window_start,
    window_end,
    window_time,
    CURRENT_WATERMARK(ts) AS current_watermark
  FROM (
    SELECT
    *
    FROM TUMBLE(
      TABLE orders_cte,
      DESCRIPTOR(ts),
      INTERVAL '5' MINUTE
    )
  )
)
WHERE rownum = 1
  AND order_id IS NOT NULL
;
```