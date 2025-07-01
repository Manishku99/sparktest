```python
import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, BooleanType

## DEFINE SENSITIVE VARIABLES
NESSIE_SERVER_URI = "http://172.18.0.4:19120/api/v2"
WAREHOUSE_BUCKET = "s3://warehouse"
MINIO_URI = "http://172.18.0.2:9000"


## Configurations for Spark Session
conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
  		#packages
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131')
  		#SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
  		#Configuring Catalog
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_SERVER_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set("spark.sql.catalog.nessie.s3.endpoint",MINIO_URI)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE_BUCKET)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
)

## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

schema = StructType([StructField('name',StringType(),True),  
        StructField('age',IntegerType(),True),
        StructField('version',IntegerType(),True),
        StructField('price',IntegerType(),True),
        StructField('address',StructType([StructField('street',StringType(),True),  
                StructField('city',StringType(),True),  
                StructField('state',StringType(),True),  
                StructField('postalCode',StringType(),True)]),True),  
        StructField('hobbies',ArrayType(StringType()),True),  
        StructField('fruits',ArrayType(StringType()),True),  
        StructField('vegetables',ArrayType(StructType([StructField('veggieName',StringType(),True),  
        StructField('veggieLike',BooleanType(),True)])),True)])





```


```python
sampledata = [
    ('A1',30,1,100,('123 Main St','New York','NY','10001'),(['reading', 'running']),(['apple', 'orange', 'pear']),([('potato',True),('broccoli',False)])),
    ('A2',31,1,100,('123 Main St','New York','NY','10001'),(['reading', 'running']),(['apple', 'orange', 'pear']),([('potato',True),('broccoli',False)]))
]

df = spark.createDataFrame(sampledata, schema)
```


```python
# Write the DataFrame creating the IcerBerg table
df.writeTo("nessie.bitemporal6").using("iceberg").create()
```


```python
spark.sql("""
  SELECT * FROM nessie.bitemporal6;
""").show()
```

    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    |name|age|version|price|             address|           hobbies|              fruits|          vegetables|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    |  A1| 30|      1|  100|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|
    |  A2| 31|      1|  100|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    



```python
spark.sql("""
  SELECT committed_at,snapshot_id,parent_id,operation,manifest_list FROM nessie.bitemporal6.snapshots;
""").show(truncate=False)
```

    +-----------------------+-------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------+
    |committed_at           |snapshot_id        |parent_id|operation|manifest_list                                                                                                                                |
    +-----------------------+-------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------+
    |2025-07-01 17:50:55.982|7341907959947901379|NULL     |append   |s3://warehouse/bitemporal6_f9998bc2-ea66-48e8-ab6e-5772f4d455ec/metadata/snap-7341907959947901379-1-cae97b23-c0fd-40c6-bcb2-9c71c600b40b.avro|
    +-----------------------+-------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------+
    



```python
#Later
sampledata_updates = [
    ('A1',30,2,200,('123 Main St','New York','NY','10001'),(['reading', 'running']),(['apple', 'orange', 'pear']),([('potato',True),('broccoli',False)])),
    ('A3',31,1,302,('123 Main St','New York','NY','10001'),(['reading', 'running']),(['apple', 'orange', 'pear']),([('potato',True),('broccoli',False)]))
]

df = spark.createDataFrame(sampledata_updates, schema)
# Write the DataFrame creating the IcerBerg table
df.createOrReplaceTempView('sampledata_upserts')
```


```python
spark.sql("""
  SELECT * FROM sampledata_upserts;
""").show()
```

    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    |name|age|version|price|             address|           hobbies|              fruits|          vegetables|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    |  A1| 30|      2|  200|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|
    |  A3| 31|      1|  302|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    



```python
spark.sql("""
  MERGE INTO nessie.bitemporal6 a
  USING (SELECT * FROM sampledata_upserts) b
      ON a.name = b.name
  WHEN MATCHED THEN
      UPDATE SET a.version = b.version, a.price = b.price
  WHEN NOT MATCHED THEN INSERT *
""").show()
```

    ++
    ||
    ++
    ++
    



```python
spark.sql("""
  SELECT * FROM nessie.bitemporal6;
""").show()
```

    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    |name|age|version|price|             address|           hobbies|              fruits|          vegetables|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    |  A1| 30|      2|  200|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|
    |  A3| 31|      1|  302|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|
    |  A2| 31|      1|  100|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+
    



```python
spark.sql("""
  SELECT committed_at,snapshot_id,parent_id,operation FROM nessie.bitemporal5.snapshots;
""").show(truncate=False)
```

    +-----------------------+-------------------+-------------------+---------+
    |committed_at           |snapshot_id        |parent_id          |operation|
    +-----------------------+-------------------+-------------------+---------+
    |2025-07-01 15:50:47.102|6232935359169789375|NULL               |append   |
    |2025-07-01 16:00:09.225|4049902090392161895|6232935359169789375|overwrite|
    |2025-07-01 17:13:54.298|3108651399773000121|4049902090392161895|overwrite|
    |2025-07-01 17:19:03.831|3342005150864703808|3108651399773000121|overwrite|
    +-----------------------+-------------------+-------------------+---------+
    



```python
spark.sql("""
  SELECT committed_at,snapshot_id,parent_id,operation,manifest_list FROM nessie.bitemporal5.snapshots;
""").show(truncate=False)
```

    +-----------------------+-------------------+-------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------+
    |committed_at           |snapshot_id        |parent_id          |operation|manifest_list                                                                                                                                |
    +-----------------------+-------------------+-------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------+
    |2025-07-01 15:50:47.102|6232935359169789375|NULL               |append   |s3://warehouse/bitemporal5_2bf5f901-69c8-4246-a9c6-173be4f7cd84/metadata/snap-6232935359169789375-1-ee2b932e-88c2-42de-b69c-c0b0c28af913.avro|
    |2025-07-01 16:00:09.225|4049902090392161895|6232935359169789375|overwrite|s3://warehouse/bitemporal5_2bf5f901-69c8-4246-a9c6-173be4f7cd84/metadata/snap-4049902090392161895-1-a2b7e112-c131-47fa-92dc-9bcb195289f3.avro|
    |2025-07-01 17:13:54.298|3108651399773000121|4049902090392161895|overwrite|s3://warehouse/bitemporal5_2bf5f901-69c8-4246-a9c6-173be4f7cd84/metadata/snap-3108651399773000121-1-e5b477d9-2544-46bc-9776-548dfd4d599a.avro|
    |2025-07-01 17:19:03.831|3342005150864703808|3108651399773000121|overwrite|s3://warehouse/bitemporal5_2bf5f901-69c8-4246-a9c6-173be4f7cd84/metadata/snap-3342005150864703808-1-4513fdd1-93cd-4a5f-b166-4f541ffd448f.avro|
    +-----------------------+-------------------+-------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------+
    



```python
spark.sql("""
  CALL nessie.system.create_changelog_view(
      table => 'nessie.bitemporal6',
      changelog_view => 'bitemporal6_view',
      identifier_columns => array('name','version')
  )
""").show()
```

    +----------------+
    |  changelog_view|
    +----------------+
    |bitemporal6_view|
    +----------------+
    



```python
spark.sql("""
  SELECT * FROM bitemporal6_view where name='A1';
""").show()
```

    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+------------+---------------+-------------------+
    |name|age|version|price|             address|           hobbies|              fruits|          vegetables|_change_type|_change_ordinal|_commit_snapshot_id|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+------------+---------------+-------------------+
    |  A1| 30|      1|  100|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|      INSERT|              0|7341907959947901379|
    |  A1| 30|      1|  100|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|      DELETE|              1|4306515488692975043|
    |  A1| 30|      2|  200|{123 Main St, New...|[reading, running]|[apple, orange, p...|[{potato, true}, ...|      INSERT|              1|4306515488692975043|
    +----+---+-------+-----+--------------------+------------------+--------------------+--------------------+------------+---------------+-------------------+
    



```python
spark.sql("""
   SELECT
        clv.name,clv.age,clv.version,clv.price,clv._change_type,clv._change_ordinal,clv._commit_snapshot_id,
        s.snapshot_id,
        s.committed_at,
        s.committed_at as effective_start
    FROM bitemporal6_view clv
    JOIN nessie.bitemporal6.snapshots s
    ON clv._commit_snapshot_id = s.snapshot_id
    WHERE (clv._change_type = 'INSERT' OR clv._change_type = 'UPDATE_AFTER')
""").show(truncate=False)
```

    +----+---+-------+-----+------------+---------------+-------------------+-------------------+-----------------------+-----------------------+
    |name|age|version|price|_change_type|_change_ordinal|_commit_snapshot_id|snapshot_id        |committed_at           |effective_start        |
    +----+---+-------+-----+------------+---------------+-------------------+-------------------+-----------------------+-----------------------+
    |A1  |30 |1      |100  |INSERT      |0              |7341907959947901379|7341907959947901379|2025-07-01 17:50:55.982|2025-07-01 17:50:55.982|
    |A1  |30 |2      |200  |INSERT      |1              |4306515488692975043|4306515488692975043|2025-07-01 17:51:46.646|2025-07-01 17:51:46.646|
    |A2  |31 |1      |100  |INSERT      |0              |7341907959947901379|7341907959947901379|2025-07-01 17:50:55.982|2025-07-01 17:50:55.982|
    |A3  |31 |1      |302  |INSERT      |1              |4306515488692975043|4306515488692975043|2025-07-01 17:51:46.646|2025-07-01 17:51:46.646|
    +----+---+-------+-----+------------+---------------+-------------------+-------------------+-----------------------+-----------------------+
    



```python
spark.sql("""
   SELECT
        clv.name,clv.age,clv.version,clv.price,clv._change_type,clv._change_ordinal,clv._commit_snapshot_id,
        s.snapshot_id,
        s.committed_at,
        s.committed_at as effective_start
    FROM bitemporal6_view clv
    LEFT JOIN nessie.bitemporal6.snapshots s
    ON clv._commit_snapshot_id = s.snapshot_id
""").show(truncate=False)
```

    +----+---+-------+-----+------------+---------------+-------------------+-------------------+-----------------------+-----------------------+
    |name|age|version|price|_change_type|_change_ordinal|_commit_snapshot_id|snapshot_id        |committed_at           |effective_start        |
    +----+---+-------+-----+------------+---------------+-------------------+-------------------+-----------------------+-----------------------+
    |A1  |30 |1      |100  |INSERT      |0              |7341907959947901379|7341907959947901379|2025-07-01 17:50:55.982|2025-07-01 17:50:55.982|
    |A1  |30 |1      |100  |DELETE      |1              |4306515488692975043|4306515488692975043|2025-07-01 17:51:46.646|2025-07-01 17:51:46.646|
    |A1  |30 |2      |200  |INSERT      |1              |4306515488692975043|4306515488692975043|2025-07-01 17:51:46.646|2025-07-01 17:51:46.646|
    |A2  |31 |1      |100  |INSERT      |0              |7341907959947901379|7341907959947901379|2025-07-01 17:50:55.982|2025-07-01 17:50:55.982|
    |A3  |31 |1      |302  |INSERT      |1              |4306515488692975043|4306515488692975043|2025-07-01 17:51:46.646|2025-07-01 17:51:46.646|
    +----+---+-------+-----+------------+---------------+-------------------+-------------------+-----------------------+-----------------------+
    



```python
spark.sql("""
WITH clv_snapshots AS (
    SELECT
        clv.*,
        s.snapshot_id,
        s.committed_at,
        s.committed_at as effective_start
    FROM bitemporal6_view clv
    JOIN nessie.bitemporal6.snapshots s
    ON clv._commit_snapshot_id = s.snapshot_id
    WHERE (clv._change_type = 'INSERT' OR clv._change_type = 'UPDATE_AFTER')
) 
SELECT
    name, 
    version, 
    price, 
    effective_start,
    snapshot_id,
    CASE
        WHEN effective_start != l_part_committed_at 
            OR _change_type = 'UPDATE_AFTER' THEN l_part_committed_at
        ELSE CAST(null as timestamp)
    END as effective_end,
    CASE
        WHEN effective_start != l_part_committed_at
            OR _change_type = 'UPDATE_AFTER' 
            OR _change_type = 'DELETE' THEN CAST(false as boolean)
        ELSE CAST(true as boolean)
    END as is_current
FROM (SELECT *, MAX(committed_at) OVER (PARTITION BY name) as l_part_committed_at FROM clv_snapshots)
WHERE _change_type != 'UPDATE_AFTER'
ORDER BY name,  _change_ordinal
""").show(truncate=False)
```

    +----+-------+-----+-----------------------+-------------------+-----------------------+----------+
    |name|version|price|effective_start        |snapshot_id        |effective_end          |is_current|
    +----+-------+-----+-----------------------+-------------------+-----------------------+----------+
    |A1  |1      |100  |2025-07-01 17:50:55.982|7341907959947901379|2025-07-01 17:51:46.646|false     |
    |A1  |2      |200  |2025-07-01 17:51:46.646|4306515488692975043|NULL                   |true      |
    |A2  |1      |100  |2025-07-01 17:50:55.982|7341907959947901379|NULL                   |true      |
    |A3  |1      |302  |2025-07-01 17:51:46.646|4306515488692975043|NULL                   |true      |
    +----+-------+-----+-----------------------+-------------------+-----------------------+----------+
    



```python

```
