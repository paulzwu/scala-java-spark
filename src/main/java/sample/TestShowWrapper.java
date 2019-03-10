package sample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestShowWrapper {

    public static void main(String s[]) {
        SparkSession spark
                = SparkSession.builder()
                .config("spark.sql.warehouse.dir", "file:///temp")
                .config("spark.driver.memory", "5g")

                .master("local[*]").appName("Spark2JdbcDs").getOrCreate();
        Dataset<Row> ds = spark.sql("Select 1 a");
        String t = ShowWrapper.getString(ds);
        System.out.println(t);

    }
}
