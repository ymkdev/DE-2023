import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;

import java.time.LocalDate;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20210313
{
        public static class UBERStudent20210313Mapper extends Mapper<Object, Text, Text, Text>
        {
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException
                {
                        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
                        Text outputKey = new Text();
                        Text outputValue = new Text();
                        String joinKey = "";
                        String o_value = "";
                        joinKey = itr.nextToken();
			o_value = itr.nextToken()+","+itr.nextToken()+","+itr.nextToken();
                        outputKey.set( joinKey );
                        outputValue.set( o_value );
                        context.write( outputKey, outputValue );
                }
                                                                                           
	}
  	public static class UBERStudent20210313Reducer extends Reducer<Text,Text,Text,Text>
        {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
                {

                        Text reduce_key = new Text();
                        Text reduce_result = new Text();
                        String dateS = "";
			int year=0;
			int month=0;
			int day=0;
			String answer="";
			String vehicles="";
			String trips="";

                        for (Text val : values) {
                                StringTokenizer itr2 = new StringTokenizer(val.toString(), ",");
			       	dateS= itr2.nextToken();
				StringTokenizer itr3 = new StringTokenizer(dateS, "/");
				day =Integer.parseInt(itr3.nextToken());
			       	month =Integer.parseInt(itr3.nextToken());
				year = Integer.parseInt(itr3.nextToken());	
				LocalDate date = LocalDate.of(year, month, day);
    

       				 // 2. DayOfWeek 객체 구하기
       				DayOfWeek dayOfWeek = date.getDayOfWeek();

        			// 3. 숫자 요일 구하기
        			//int dayOfWeekNumber = dayOfWeek.getValue();
                               
				if(dayOfWeek.getValue() == 1) {
					answer = "MON";
				}else if (dayOfWeek.getValue() == 2) {
					answer = "TUE";
				}else if (dayOfWeek.getValue() == 3) {
					answer = "WED";
				}else if (dayOfWeek.getValue() == 4) {
					answer = "THR";
				}else if (dayOfWeek.getValue() == 5) {
					answer = "FRI";
				}else if (dayOfWeek.getValue() == 6) {
					answer = "SAT";
				}else if (dayOfWeek.getValue() == 7) {
					answer = "SUN";
				}       

				reduce_key.set(key+","+answer);
				vehicles = itr2.nextToken();
				trips = itr2.nextToken();
				reduce_result.set(trips+","+vehicles);
				context.write(reduce_key, reduce_result);
                        }

                }
        }
        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 2)
                {
                        System.err.println("Usage: wordcount <in> <out>");
                        System.exit(2);
                }
                Job job = new Job(conf, "UBERStudent20210313");
                job.setJarByClass(UBERStudent20210313.class);
                job.setMapperClass(UBERStudent20210313Mapper.class);
                job.setCombinerClass(UBERStudent20210313Reducer.class);
                job.setReducerClass(UBERStudent20210313Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
