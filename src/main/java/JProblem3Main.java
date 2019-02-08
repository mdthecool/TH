import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class JProblem3Main {

    public static void main(String args[]) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JProblem1Main").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sc = new SQLContext(jsc);       
	//   **************************** YOUR CODE STARTS HERE  ************************************** 
	 
        Dataset<Row> df  = sc.read().load(args[0]);
        df.show();		
	System.out.println(df.count());
    }

}
