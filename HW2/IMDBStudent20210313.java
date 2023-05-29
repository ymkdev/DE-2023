import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class IMDB{
	public String description;
	public double avg_rating;
	public IMDB(String description, double avg_rating) {
		this.description = description;
   		this.avg_rating = avg_rating;
  	}
}
public class IMDBStudent20210313
{
	public static class IMDBStudent20210313Comparator implements Comparator<IMDB>
        {
                public int compare(IMDB x, IMDB y) {
                        if ( x.avg_rating > y.avg_rating ) return 1;
                        if ( x.avg_rating < y.avg_rating ) return -1;
                        return 0;
                }
        }
	
	public static void insertIMDBStudent20210313(PriorityQueue q, String description, double avg_rating, int topK)
        {
                IMDB IMDBStudent20210313_head = (IMDB) q.peek();
                if ( q.size() < topK || IMDBStudent20210313_head.avg_rating < avg_rating )
                {
                        IMDB imdbstudent20210313 = new IMDB(description, avg_rating);
                        q.add( imdbstudent20210313 );
                        if( q.size() > topK ) q.remove();
                }
        }
	
        public static class IMDBStudent20210313Mapper extends Mapper<Object, Text, Text, Text>
        {
                boolean fileA = true;
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException
                {
                        StringTokenizer itr = new StringTokenizer(value.toString(), "::");
                        Text outputKey = new Text();
                        Text outputValue = new Text();
                        String joinKey = "";
                        String o_value = "";
                        if( fileA ) {
				joinKey = itr.nextToken();
                                o_value="A,"+itr.nextToken();
                        }
                        else {
				itr.nextToken();
                                joinKey = itr.nextToken();
                                o_value="B,"+itr.nextToken();
                        }
                        outputKey.set( joinKey );
                        outputValue.set( o_value );
                        context.write( outputKey, outputValue );
                }
                protected void setup(Context context) throws IOException, InterruptedException
                {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

                        if ( filename.indexOf( "movies.dat" ) != -1 ) fileA = true;
                                else fileA = false;
                }
	}

	public static class IMDBStudent20210313Reducer extends Reducer<Text,Text,Text,DoubleWritable>
        {
                private PriorityQueue<IMDB> queue ;
                private Comparator<IMDB> comp = new IMDBStudent20210313Comparator();
                private int topK;
	       	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,

                InterruptedException
                {

                        Text reduce_key = new Text();
                        Text reduce_result = new Text();
                        String description2 = "";
			double avg_rating2=0;
			int count2=0;
                        ArrayList<String> buffer = new ArrayList<String>();

                        for (Text val : values) {
                                StringTokenizer itr2 = new StringTokenizer(val.toString(), ",");
                                String file_type="";
                                if( itr2.nextToken().equals( "A" ) ) {
                                        description2 = itr2.nextToken();
                                }
                                else
                                {
                                        if ( description2.length() == 0 ) {
                                                buffer.add( val.toString() );
                                        }
                                        else {
                                             //   reduce_key.set(description);
					     	avg_rating2 +=Double.parseDouble( itr2.nextToken());
                                            //    reduce_result.set(itr2.nextToken()+" "+description)
                                           //     context.write(reduce_key, reduce_result);
						count2++;
                                        }
                                }
                        }

                        for ( int i = 0 ; i < buffer.size(); i++ )
                        {
                                String vals="";
                                vals =buffer.get(i);
                                Text val3=new Text();
                                val3.set(vals);
                                StringTokenizer itr4 = new StringTokenizer(val3.toString(), ",");
                                itr4.nextToken();
				avg_rating2 += Double.parseDouble(itr4.nextToken());
                              //  reduce_key.set(itr4.nextToken());
                             //   reduce_result.set(itr4.nextToken()+" "+description);
                             //   context.write(reduce_key, reduce_result);
			     	count2++;

                        }
		//	reduce_key.set(description);
		//	reduce_result.set((double)avg_rating/count);
		//	context.write(rudece_key, reduce_result);
			double avg_rating3 = (double)avg_rating2/count2;
			insertIMDBStudent20210313(queue, description2, avg_rating3, topK);
                }
		protected void setup(Context context) throws IOException, InterruptedException {
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<IMDB>( topK , comp);
                }
                protected void cleanup(Context context) throws IOException, InterruptedException {
                        while( queue.size() != 0 ) {
                                IMDB imdbstudent = (IMDB) queue.remove();
                                context.write( new Text( imdbstudent.description ), new DoubleWritable(imdbstudent.avg_rating)); 
                        }
                }

        }


	public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
              // int topK = 2;
                if (otherArgs.length != 3)
                {
                        System.err.println("Usage: ReduceSideJoin <in> <out>");
                        System.exit(2);
                }
               // conf.setInt("topK", topK);
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));

                Job job = new Job(conf, "IMDBStudent20210313");
                job.setJarByClass(IMDBStudent20210313.class);
                job.setMapperClass(IMDBStudent20210313Mapper.class);
                job.setReducerClass(IMDBStudent20210313Reducer.class);
		job.setNumReduceTasks(1);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
                FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}

