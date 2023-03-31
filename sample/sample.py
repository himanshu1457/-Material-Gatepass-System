import org.apache.spark.sql.types.Decimal

val handleBigDecZeroUDF = udf((decimalVal:Decimal) => {
    if (decimalVal.scale > 6) {
    decimalVal.toBigDecimal.bigDecimal.toPlainString()
  } else {
    decimalVal.toString()
  }
})   
 
spark.sql("create table testBigDec (a decimal(10,7), b decimal(10,6), c decimal(10,8))")
spark.sql("insert into testBigDec values(0, 0,0)")
spark.sql("insert into testBigDec values(1, 1, 1)")
val df = spark.table("testBigDec")
df.show(false) // this will show scientific notation

// use custom UDF `handleBigDecZeroUDF` to convert zero into plainText notation 
 df.select(handleBigDecZeroUDF(col("a")).as("a"),col("b"),handleBigDecZeroUDF(col("c")).as("c")).show(false)
