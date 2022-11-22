# data filtering
Only want data which are:
1. trip distance > 0
2. payment type = 1 (pay by credit card)

# wanted features
> 小费相关的
- tip_amount
- fare_amount
- tip/fare ratio ( tip_amount / fare_amount)
- tip/total ratio (tip_amount / total_amount)
- passenger_count
- trip_distance
- travel speed (trip_distance / (tpep_dropoff_time - tpep_pickup_time)) 
- year of pickup date 
- dayofweek(tpep_pickup_time)
- Pickup hour of the day
``` python
from pyspark.sql import functions as F
df.withColumn("hour", F.date_trunc('hour',F.to_timestamp("timestamp","yyyy-MM-dd HH:mm:ss 'UTC'")))\
  .show(truncate=False)
```
- is_holiday (找到了panda的包 应该要写udf https://stackoverflow.com/questions/2394235/detecting-a-us-holiday)

- Lattitude difference
- Longitude difference
- Geohashed pickup location
- Geohashed dropoff location
> 参考了这个项目 https://towardsdatascience.com/new-york-taxi-data-set-analysis-7f3a9ad84850
- pickup/drop down 在商区或者富人区？？

