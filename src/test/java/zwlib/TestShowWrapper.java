package zwlib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import static org.junit.Assert.*;
public class TestShowWrapper {

    @Test
    public void main() {
        SparkSession spark
                = SparkSession.builder()
                .config("spark.sql.warehouse.dir", "file:///temp")
                .config("spark.driver.memory", "5g")

                .master("local[*]").appName("Spark2JdbcDs").getOrCreate();
        Dataset<Row> ds = spark.sql("Select 1 a");
        String t = ShowWrapper.getString(ds);
        String expected = "+---+\n" +
                "|  a|\n" +
                "+---+\n" +
                "|  1|\n" +
                "+---+\n\r\n";
        System.out.println(expected.length() + " " + t.length());
        for (int i=0; i<expected.length(); i++) {
            System.out.println((int)expected.charAt(i) + "=" +(int) t.charAt(i));
            }

        assertEquals(expected, t);
    }
}
