import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
  // add code here
  public static class Task4Mapper
      extends Mapper<Object, Text, Text, IntWritable> {
    private HashMap<String, ArrayList<Integer>> movieRatings
      = new HashMap<String, ArrayList<Integer>>();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	    String[] columns = value.toString().split(",");
      ArrayList<Integer> ratings = new ArrayList<Integer>();
      for (int i = 1; i < columns.length; ++i) {
        if (columns[i].equals("")) {
          ratings.add(0);
        } else {
          ratings.add(Integer.parseInt(columns[i]));
        }
	    }
      for (String movie : movieRatings.keySet()) {
        int numSameRatings = 0;
        for (int i = 1; i < ratings.size(); ++i) {
          if (ratings.get(i) != 0 && ratings.get(i) == movieRatings.get(movie).get(i)) {
            ++numSameRatings;
          }
        }
        if (movie.compareTo(columns[0]) > 0) {
          context.write(new Text(columns[0] + "," + movie), new IntWritable(numSameRatings));
        } else {
          context.write(new Text(movie + "," + columns[0]), new IntWritable(numSameRatings));
        }
      }
      movieRatings.put(columns[0], ratings);
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(Task4Mapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
