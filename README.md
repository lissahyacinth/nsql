# Querying NATS JetStream using a KSQL-like language

## Current things that are doable

`SELECT StringPayload FROM streamA WHERE CorrelationID = 1`

## Ideal queries when this is finished
```
CREATE STREAM user_purchases AS
  SELECT u.user_id,
         u.name,
         p.product_id,
         p.amount,
         p.timestamp
  FROM user_stream u
  INNER JOIN purchase_stream p 
    WITHIN 1 HOUR
    ON u.user_id = p.user_id
  EMIT CHANGES;
```