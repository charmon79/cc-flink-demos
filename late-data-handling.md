# Late Data Handling

In Flink SQL, any query which relies on event-time based processing relies on watermark progression to determine when to emit results. This includes:

* Temporal JOINs
* Interval JOINs
* Window TVFs (i.e. TUMBLE, HOP, CUMULATE, SESSION)
* OVER aggregations

If new records arrive which have a timestamp <= the current watermark, Flink considers these records to be "late", and they are ignored.

While it's possible to specify a watermark delay to allow for out-of-order data to be processed correctly, you may still have records which arrive much later than this allows for. A classic example is when processing data from edge systems which could experience extended network interruptions before they're able to synchronize data to the cloud. It is likely unavoidable that some data could arrive potentially hours out of order. It's also usually not desirable to specify a watermark delay interval on the order of hours, because this means hours of added latency before results are produced.

Flink SQL doesn't currently provide a seamless way to handle late events. If a statement relies on watermarks to make progress, it will simply ignore late records & they will therefore not be reflected in the statement's output.

However, regular streaming queries (i.e. a simple SELECT) which _don't_ rely on event time will not ignore anything. Therefore, by leveraging statement sets, we can siphon off late records to another table, as a sort of DLQ.

## Demo

### Create some late data

Confluent Cloud provides some example tables that generate sample data. We can use these to create a stream of `orders` events where ~10% of records are out of order by 5 minutes.

```
/* Generate data with 10% of events arriving out-of-order */
CREATE TABLE orders_ooo_test (
  WATERMARK FOR order_ts AS order_ts - INTERVAL '0.001' SECOND
)
  DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
  AS
SELECT
  order_id,
  customer_id,
  product_id,
  price,
  CASE
    WHEN RAND() <= 0.1 THEN TIMESTAMPADD(MINUTE, -5, `$rowtime`)
    ELSE `$rowtime`
  END AS order_ts
FROM examples.marketplace.orders
;
```

### Create a "DLQ" table to hold late records

```
CREATE TABLE orders_ooo_test_late_dlq (
  order_id string,
  customer_id int,
  product_id string,
  price double,
  order_ts TIMESTAMP_LTZ(3),
  src_partition BIGINT,
  src_offset BIGINT,
  wm TIMESTAMP_LTZ(3)
)
DISTRIBUTED BY (order_id) INTO 1 BUCKETS;
```

### Create sink table for hourly windows

```
CREATE TABLE hourly_totals_by_customer (
  customer_id INT NOT NULL,
  order_count BIGINT NOT NULL,
  order_total DOUBLE NOT NULL,
  window_start TIMESTAMP(3) NOT NULL,
  window_end TIMESTAMP(3) NOT NULL
)
DISTRIBUTED BY (customer_id) INTO 1 BUCKETS
;
```

### Shunt late data to DLQ & process timely data into hourly windows

This will only read each source record once, and either send it to the DLQ table (if it's late), or process it normally in a TUMBLE window aggregation query and route to the sink table (if it's within the bounded out-of-orderness defined in the source table's watermarking strategy).

```
EXECUTE STATEMENT SET
BEGIN

  -- Route late records to DLQ table
  INSERT INTO orders_ooo_test_late_dlq
  SELECT
    order_id,
    customer_id,
    product_id,
    price,
    order_ts,
    `partition` AS src_partition,
    `offset` AS src_offset,
    CURRENT_WATERMARK(order_ts) AS wm
  FROM orders_ooo_test
  WHERE order_ts <= CURRENT_WATERMARK(order_ts);

  -- Process timely records and insert into sink table
  INSERT INTO hourly_totals_by_customer
  SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(price) AS order_total,
    window_start,
    window_end
  FROM TUMBLE(
    TABLE orders_ooo_test,
    DESCRIPTOR(order_ts),
    INTERVAL '1' HOUR
  )
  WHERE order_ts > CURRENT_WATERMARK(order_ts)
  GROUP BY
    window_start,
    window_end,
    customer_id
  ;

END;
```

### Bonus: Use a subsequent Flink SQL statement to process the late records

If we want, we can use a subsequent Flink SQL statement to union the late-arriving records to the result of the hourly windowed aggregation, then use a GROUP BY statement to effectively "assign" them to the appropriate windows & emit updated aggregates.

NOTE: This method is only feasible for TUMBLE windows, because they are non-overlapping (unlike HOP or CUMULATE windows) and have deterministic boundaries (unlike SESSION windows).

Be mindful that Flink needs to maintain more state for this statement, since it's a regular, unbounded GROUP BY aggregation. The result will also be a `retract` changelog, because groups need to be updated as new rows are processed.

```
SET 'sql.state-ttl' = '7d';

CREATE TABLE updating_hourly_windows
DISTRIBUTED BY (customer_id) INTO 1 BUCKETS
  AS
SELECT
  customer_id,
  SUM(order_count) order_count,
  SUM(order_total) order_total,
  MAX(window_start) window_start,
  MAX(window_end) window_end
FROM (
  SELECT
    customer_id,
    order_count,
    order_total,
    window_start,
    window_end
  FROM hourly_totals_by_customer
  UNION ALL
  SELECT
    customer_id,
    1 AS order_count,
    price AS order_total,
    order_ts AS window_start,
    order_ts AS window_end
  FROM orders_ooo_test_late_dlq
)
GROUP BY
  customer_id,
  DATE_FORMAT(window_start, 'yyyy-MM-dd:HH')
```