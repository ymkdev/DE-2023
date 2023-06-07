import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import scala.Tuple2;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.StringTokenizer;

public final class UBERStudent20210313 implements Serializable {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: UBERStudent20210313 <in-file> <out-file>");
      System.exit(1);
    }
    SparkSession spark = SparkSession
      .builder()
      .appName("UBERStudent")
      .getOrCreate();

    JavaRDD<String> products = spark.read().textFile(args[0]).javaRDD();

    PairFunction<String, String, String> pfA = new PairFunction<String, String, String>() {
      public Tuple2<String, String> call(String s) {
        StringTokenizer itr = new StringTokenizer(s, ",");
        String outputKey = "";
        String outputValue = "";

        String joinKey = "";
        String o_value = "";
        String dateS = "";
        int year = 0;
        int month = 0;
        int day = 0;
        String answer = "";
        String vehicles = "";
        String trips = "";

        joinKey = itr.nextToken();
        dateS = itr.nextToken();
        StringTokenizer itr3 = new StringTokenizer(dateS, "/");
        month = Integer.parseInt(itr3.nextToken());
        day = Integer.parseInt(itr3.nextToken());
        year = Integer.parseInt(itr3.nextToken());
        LocalDate date = LocalDate.of(year, month, day);
        DayOfWeek dayOfWeek = date.getDayOfWeek();
        if (dayOfWeek.getValue() == 1) {
          answer = "MON";
        } else if (dayOfWeek.getValue() == 2) {
          answer = "TUE";
        } else if (dayOfWeek.getValue() == 3) {
          answer = "WED";
        } else if (dayOfWeek.getValue() == 4) {
          answer = "THR";
        } else if (dayOfWeek.getValue() == 5) {
          answer = "FRI";
        } else if (dayOfWeek.getValue() == 6) {
          answer = "SAT";
        } else if (dayOfWeek.getValue() == 7) {
          answer = "SUN";
        }
        joinKey = joinKey + "," + answer;
        outputKey = joinKey;
        vehicles = itr.nextToken();
        trips = itr.nextToken();
        outputValue = trips + "," + vehicles;

        return new Tuple2<>(outputKey, outputValue);
      }
    };

    JavaPairRDD<String, String> pTuples = products.mapToPair(pfA);

    Function2<String, String, String> f2 = new Function2<String, String, String>() {
      public String call(String x, String y) {
        String reduce_result = "";
        int sum = 0;
        int sum2 = 0;

        StringTokenizer itr5 = new StringTokenizer(x, ",");
        int n1 = Integer.parseInt(itr5.nextToken());
        int n2 = Integer.parseInt(itr5.nextToken());

        StringTokenizer itr6 = new StringTokenizer(y, ",");
        int k1 = Integer.parseInt(itr6.nextToken());
        int k2 = Integer.parseInt(itr6.nextToken());

        sum = n1 + k1;
        sum2 = n2 + k2;

        reduce_result = sum + "," + sum2;

        return reduce_result;
      }
    };

    JavaPairRDD<String, String> counts = pTuples.reduceByKey(f2);
    counts.saveAsTextFile(args[1]);
    spark.stop();
  }
}
