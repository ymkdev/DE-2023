import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
public final class UBERStudent20210313 {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: UBERStudent20210313 <in-file> <out-file>");
      System.exit(1);
    }
    
    SparkSession spark = SparkSession
      .builder()
      .appName("UBERStudent")
      .getOrCreate();
    
    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    
    JavaPairRDD<String, String> pairs = lines.mapToPair(new PairFunction<String, String, String>() {
      public Tuple2<String, String> call(String line) {
        String[] fields = line.split(",");
        String baseNumber = fields[0];
        String dateString = fields[1];
        int vehicles = Integer.parseInt(fields[2]);
        int trips = Integer.parseInt(fields[3]);
        
        LocalDate date = LocalDate.parse(dateString);
        DayOfWeek dayOfWeek = date.getDayOfWeek();
        String dayOfWeekString = dayOfWeek.toString().substring(0, 3); // 요일을 세 글자로 변환 (e.g., MON, TUE, etc.)
        
        String key = baseNumber + "," + dayOfWeekString;
        String value = trips + "," + vehicles;
        
        return new Tuple2<>(key, value);
      }
    });
    
    JavaPairRDD<String, String> sums = pairs.reduceByKey(new Function2<String, String, String>() {
      public String call(String x, String y) {
        int tripsX = Integer.parseInt(x.split(",")[0]);
        int tripsY = Integer.parseInt(y.split(",")[0]);
        int vehiclesX = Integer.parseInt(x.split(",")[1]);
        int vehiclesY = Integer.parseInt(y.split(",")[1]);
        
        int sumTrips = tripsX + tripsY;
        int sumVehicles = vehiclesX + vehiclesY;
        
        return sumTrips + "," + sumVehicles;
      }
    });
    
    sums.saveAsTextFile(args[1]);
    
    spark.stop();
  }
}
