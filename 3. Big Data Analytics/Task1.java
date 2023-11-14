import java.io.IOException;
import java.util.ArrayList;

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

public class Task1 {

  // add code here
  public static class Task1Mapper 
      extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	    String[] columns = value.toString().split(",");
      Text movieTitle = new Text(columns[0]);
      int maxRating = 0;
	    for (int i = 1; i < columns.length; ++i) {
        if (!columns[i].equals("")) {
          int rating = Integer.parseInt(columns[i]);
          if (rating > maxRating) {
            maxRating = rating;
          }
        }
	    }
      ArrayList<Integer> users = new ArrayList<Integer>();
      for (int i = 1; i < columns.length; ++i) {
        if (!columns[i].equals("")) {
          int rating = Integer.parseInt(columns[i]);
          if (rating == maxRating) {
            users.add(i);
          }
        }
      }
      String result = "";
      if (users.size() > 0) {
        result += users.get(0).toString();
      }
      for (int i = 1; i < users.size(); ++i) {
        result += ",";
        result += users.get(i).toString();
      }
      context.write(movieTitle, new Text(result));
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // add code here
    job.setMapperClass(Task1Mapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
