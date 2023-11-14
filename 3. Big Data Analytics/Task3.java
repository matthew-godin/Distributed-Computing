import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  // add code here
  public static class Task3Mapper 
      extends Mapper<Object, Text, IntWritable, NullWritable> {
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	    String[] columns = value.toString().split(",");
	    for (int i = 1; i < columns.length; ++i) {
        if (!columns[i].equals("")) {
          context.write(new IntWritable(i), NullWritable.get());
        }
	    }
    }
  }

  public static class Task3Reducer 
       extends Reducer<IntWritable,NullWritable,IntWritable,IntWritable> {
    public void reduce(IntWritable key, Iterable<NullWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int totalNumRatings = 0;
      for (NullWritable val : values) {
        ++totalNumRatings;
      }
      context.write(key, new IntWritable(totalNumRatings));
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(Task3Mapper.class);
    job.setReducerClass(Task3Reducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
