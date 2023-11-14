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

public class Task2 {

  // add code here
  public static class Task2Mapper 
      extends Mapper<Object, Text, NullWritable, IntWritable> {
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	    String[] columns = value.toString().split(",");
      int numRatings = 0;
	    for (int i = 1; i < columns.length; ++i) {
        if (!columns[i].equals("")) {
          ++numRatings;
        }
	    }
      context.write(NullWritable.get(), new IntWritable(numRatings));
    }
  }

  public static class Task2Reducer 
       extends Reducer<NullWritable,IntWritable,NullWritable,IntWritable> {
    public void reduce(NullWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int totalNumRatings = 0;
      for (IntWritable val : values) {
        totalNumRatings += val.get();
      }
      context.write(NullWritable.get(), new IntWritable(totalNumRatings));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.job.reduces", "1");
    
    Job job = Job.getInstance(conf, "Task2");
    job.setJarByClass(Task2.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(Task2Mapper.class);
    job.setCombinerClass(Task2Reducer.class);
    job.setReducerClass(Task2Reducer.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
