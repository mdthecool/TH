import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame


object SProblem1Main {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("SProblem1Main").master("local[*]").getOrCreate()

    // Your code goes here 
    val df = spark.read.option("header", "true").csv(args(0))
    printf(""+df.count)
  }
}

