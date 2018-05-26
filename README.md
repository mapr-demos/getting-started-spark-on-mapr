# Getting Started with Spark on MapR


## Read Data from MapR-FS


1 - Copy the data into MapR File System

For this example we will use a CSV file that contains a list of auctions.

The file is located in this project: `/data/auctiondata.csv`

Copy the file to your cluster, in the `/apps/` directory using the `cp`/`scp` command or `hadoop put` for example

```
$ scp   ./data/auctiondata.csv    mapr@[mapr-cluster-node]:/mapr/[cluster-name]/apps/
```
or
```
$ hadoop fs -put ./data/auctiondata.csv /apps
```


The dataset to be used is from eBay online auctions. The eBay online auction dataset contains the following fields:

* auctionid - unique identifier of an auction
* bid - the proxy bid placed by a bidder
* bidtime - the time (in days) that the bid was placed, from the start of the auction
* bidder - eBay username of the bidder
* bidderrate - eBay feedback rating of the bidder
* openbid - the opening bid set by the seller
* price - the closing price that the item sold for (equivalent to the second highest bid + an increment)
The table below shows the fields with some sample data:

| auctionid | bid | bidtime | bidder | bidderrate | openbid | price | item | daystolive |
|----------:|----:|--------:|-------:|-----------:|--------:|------:|-----:|-----------:|
| 8213034705|  95 | 2.927373|jake7870|	         0|       95|  117.5|  xbox|           3|



2 - Use Spark Shell

`$SPARK_HOME` represent the home of your Apache Spark installation in MapR, for example: `/opt/mapr/spark/spark-2.2.1/`

```
$ $SPARK_HOME/bin/spark-shell --master local[2]
```

3 - Once the Spark shell is ready load the data the following command:

```
scala> val auctionData = spark.read.textFile("/apps/auctiondata.csv")
```


4 - Get the first entry

```
scala> auctionData.first()
```

5 - Count the number of entries

```
scala> auctionData.count()
```

6 - Use other Spark actions

```
// Displays first 20 lines
scala> auctionData.show()

// Displays first 3 lines – change value to see more/less
scala> auctionData.take(3)
```

7 - Let's now transform the Dataset into a new one that contains only xbox lines, and count them

```
scala> val auctionWithXbox = auctionData.filter(line => line.contains("xbox"))

scala> auctionWithXbox.count()
```

This could be done in a single chaining transformation and operations:

```
scala> auctionData.filter(line => line.contains("xbox")).count()
```

8 - Use Spark Dataframe 

```

scala> val auctionDataFrame = spark.read.format("csv").option("inferSchema",
true).load("/apps/auctiondata.csv").toDF("auctionid","bid","bidtime","bidder","bidderrate","openbid","price","item","daystolive")

scala> auctionDataFrame.show()

```

9 - Use a filter

```
scala> auctionDataFrame.filter($"price" < 30).show()
``` 


In these steps you have learned how to put a file into MapR File System and use Spark Shell to load and query.

## Write Data to MapR-FS

Using the same dataset let's save all all `xbox` as file into MapR File system, the filter will be `filter($"item" === "xboc")` and use `write.json` or other options to save the result of the action into MapR File System

```
scala> auctionDataFrame.filter($"item" === "xbox").write.json("/apps/results/json/xbox")
```

This command will create the `/apps/results/json/xbox` directory and you will see the JSON file(s) created.

You can use the same to create Parquet or other format.

```
scala> auctionDataFrame.filter($"item" === "xbox").write.parquet("/apps/results/parquet/xbox")
```

## Read and Write Data to MapR-DB JSON

Let's now see how we can use Spark to write and read data stored into MapR-DB JSON table.

### Write data into MapR-DB JSON

The first thing to do when you work with MapR-DB JSON is to define a document `_id` that uniquely identified the document.

None of the fields that are present in the csv are using so, you will have to add a new `_id` field, and for this we will just generate a UUID.

```
scala> import spark.implicits._

scala> import java.util.UUID

scala> import org.apache.spark.sql.SparkSession

scala> import org.apache.spark.sql.types._

scala> import org.apache.spark.sql.SaveMode

scala> import com.mapr.db.spark.sql._  // import the MapR-DB OJAI Connector

scala> val generateUUID = udf(() => UUID.randomUUID().toString) // create UDF to generate UUID

scala> // showing that you can create your own schema 
    val customSchema = 
    StructType(
        Array(
        StructField("actionid",StringType,true),
        StructField("bid",DoubleType,true), 
        StructField("bidtime",DoubleType,true),
        StructField("bidder",StringType,true),
        StructField("bidderrate",IntegerType,true),
        StructField("openbid",DoubleType,true),
        StructField("price",DoubleType,true),
        StructField("item",StringType,true),
        StructField("daystolive",IntegerType,true)
        )
        )


scala> val auctionDataFrame = spark.read.format("csv").schema(customSchema).load("/apps/auctiondata.csv").toDF()

scala> val auctionDataFrameForDB = auctionDataFrame.withColumn("_id", generateUUID())

scala> auctionDataFrameForDB.show

scala> auctionDataFrameForDB.write.option("Operation", "InsertOrReplace").saveToMapRDB("/apps/auction_json_table")


```

This will create the table and insert the data into the `/apps/auction_json_table`. You can now query the table using MapR DB Shell, in a terminal on your cluster run the following command:

```
$ mapr dbshell

maprdb mapr:> find /apps/auction_json_table --limit 10
```


### Read data from MapR-DB JSON

Now that you have data into MapR-DB JSON, Spark can create a Dataframe using the following command:

```

scala> import com.mapr.db.spark.sql._

scala> import org.apache.spark.sql.SparkSession

scala> val dataFromMapR = spark.loadFromMapRDB("/apps/auction_json_table")

scala> dataFromMapR.printSchema

scala> dataFromMapR.count


scala> dataFromMapR.filter($"price" < 30).show()   // use a filter

```