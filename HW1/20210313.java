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

public class YouTubeStudent20210313 {
  public String one;
  public String two;
  public String third;
  public String category;
  public String fifth;
  public String six;
  public double rating
  public YouTubeStudent20210313(String category, double rating) {
    this.category = category;
    this.rating = rating;
  }
  public String getString()
  {
    return category +" " +rating;
  }
  public static class YouTubeStudent20210313Comparator implements Comparator<YouTubeStudent20210313> {
    public int compare(YouTubeStudent20210313 x, YouTubeStudent20210313 y) {
      if ( x.rating > y.rating ) return 1;
      if ( x.rating < y.rating ) return -1;
      return 0;
    }
  }
  
  public static void insertYouTubeStudent20210313(PriorityQueue q, String category, double rating, int topK) {
    YouTubeStudent20210313 YouTubeStudent20210313_head = (YouTubeStudent20210313) q.peek();
    if ( q.size() < topK || YouTubeStudent20210313_head.rating < rating )
    {
      YouTubeStudent20210313 youtubestudent20210313 = new YouTubeStudent20210313(category, rating);
      q.add( youtubestudent20210313 );
      if( q.size() > topK ) q.remove();
    }
  }
  
  public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable> {
    private PriorityQueue<YouTubeStudent20210313> queue ;
    private Comparator<YouTubeStudent20210313> comp = new YouTubeStudent20210313Comparator();
    private int topK;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "|");
      itr.nextToken();
      itr.nextToken();
      itr.nextToken();
      itr.nextToken();
      String category = itr.nextToken();
      itr.nextToken();
      itr.nextToken();
      double rating = Double.parseDouble(itr.nextToken());
      insertYouTubeStudent20210313(queue, category, rating, topK);
  }
  
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    topK = conf.getInt("topK", -1);
    queue = new PriorityQueue<YouTubeStudent20210313>( topK , comp);
  }
    
  protected void cleanup(Context context) throws IOException, InterruptedException {
    while( queue.size() != 0 ) {
      YouTubeStudent20210313 youtubestudent20210313 = (YouTubeStudent20210313) queue.remove();
      context.write( new Text( youtubestudent20210313.getString() ), NullWritable.get() );
    }
  }
    
  public static class TopKReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    private PriorityQueue<YouTubeStudent20210313> queue ;
    private Comparator<YouTubeStudent20210313> comp = new YouTubeStudent20210313Comparator();
    private int topK;
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(key.toString(), " ");
      //itr.nextToken();
      //itr.nextToken();
      //itr.nextToken();
      //itr.nextToken();
      String category = itr.nextToken();
      //itr.nextToken();
      //itr.nextToken();
      double rating = Double.parseDouble(itr.nextToken());
      insertYouTubeStudent20210313(queue, category, rating, topK);
  }
  
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    topK = conf.getInt("topK", -1);
    queue = new PriorityQueue<YouTubeStudent20210313>( topK , comp);
  }
    
  protected void cleanup(Context context) throws IOException, InterruptedException {
    while( queue.size() != 0 ) {
      YouTubeStudent20210313 youtubestudent20210313 = (YouTubeStudent20210313) queue.remove();
      context.write( new Text( youtubestudent20210313.getString() ), NullWritable.get() );
    }
  }
    
  public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int topK = 2;
    if (otherArgs.length != 2) {
      System.err.println("Usage: TopK <in> <out>"); System.exit(2);
    }
    conf.setInt("topK", topK);
    Job job = new Job(conf, "TopK");
    job.setJarByClass(TopK.class);
    job.setMapperClass(TopKMapper.class);
    job.setReducerClass(TopKReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
