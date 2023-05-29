import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class YouTube{
	public String category;
	public double rating;
	public YouTube(String category, double rating) {
		this.category = category;
   		this.rating = rating;
  	}
}
public class YouTubeStudent20210313 {
 	
  	public static class YouTubeStudent20210313Comparator implements Comparator<YouTube> {
    		public int compare(YouTube x, YouTube y) {
      			if ( x.rating > y.rating ) return 1;
      			if ( x.rating < y.rating ) return -1;
      			return 0;
    		}
  	}
  
 	 public static void insertYouTubeStudent20210313(PriorityQueue q, String category, double rating, int topK) {
	    	YouTube YouTubeStudent20210313_head = (YouTube) q.peek();
    		if ( q.size() < topK || YouTubeStudent20210313_head.rating < rating )
    		{
      			YouTube youtubestudent20210313 = new YouTube(category, rating);
      			q.add( youtubestudent20210313 );
      			if( q.size() > topK ) q.remove();
    		}
  	}
  
 	 public static class YouTubeStudent20210313Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
   	//	 private PriorityQueue<YouTubeStudent20210313> queue ;
  	//	 private Comparator<YouTubeStudent20210313> comp = new YouTubeStudent20210313Comparator();
    	//	 private int topK;
    		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
     			itr.nextToken();
      			itr.nextToken();
      			itr.nextToken();
      			//itr.nextToken();
     			String category = itr.nextToken();
      			itr.nextToken();
      			itr.nextToken();
      			double rating = Double.parseDouble(itr.nextToken());
      			//insertYouTubeStudent20210313(queue, category, rating, topK);
			context.write(new Text(category), new DoubleWritable(rating));
  		}
  
  		/*protected void setup(Context context) throws IOException, InterruptedException {
    			Configuration conf = context.getConfiguration();
    			topK = conf.getInt("topK", -1);
    			queue = new PriorityQueue<YouTubeStudent20210313>( topK , comp);
  		}
    
  		protected void cleanup(Context context) throws IOException, InterruptedException {
    			while( queue.size() != 0 ) {
      			YouTubeStudent20210313 youtubestudent20210313 = (YouTubeStudent20210313) queue.remove();
      			context.write( new Text( youtubestudent20210313.getString() ), NullWritable.get() );
    			}
  		}*/
	}
	public static class YouTubeStudent20210313Reducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    		private PriorityQueue<YouTube> queue ;
    		private Comparator<YouTube> comp = new YouTubeStudent20210313Comparator();
    		private int topK;
    		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      			double sum =0;
			double average=0;
			int count =0;
			
			for(DoubleWritable val : values){
				sum += val.get();
				count++;
			}
			average = sum/count;
		
      			insertYouTubeStudent20210313(queue, key.toString(), average, topK);
  		}
  
  		protected void setup(Context context) throws IOException, InterruptedException {
    			Configuration conf = context.getConfiguration();
    			topK = conf.getInt("topK", -1);
    			queue = new PriorityQueue<YouTube>( topK , comp);
  		}
    
  		protected void cleanup(Context context) throws IOException, InterruptedException {
    			while( queue.size() != 0 ) {
      				YouTube youtubestudent = (YouTube) queue.remove();
      				context.write( new Text( youtubestudent.category ), new DoubleWritable(youtubestudent.rating) );
    			}
  		}
	}
	public static void main(String[] args) throws Exception{
    		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    		//int topK = 2;
    		if (otherArgs.length != 3) {
    	  		System.err.println("Usage: TopK <in> <out>"); System.exit(2);
    		}
    		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
    		Job job = new Job(conf, "YouTubeStudent20210313");
    		job.setJarByClass(YouTubeStudent20210313.class);
    		job.setMapperClass(YouTubeStudent20210313Mapper.class);
    		job.setReducerClass(YouTubeStudent20210313Reducer.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(DoubleWritable.class);
    		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
