package uam;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PlayerAge {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private Text team = new Text();
    private IntWritable age = new IntWritable();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String[] split = value.toString().split(",");
          team.set(split[1]);
          age.set(split[2]);
          context.write(team, age);
      }
  }

  public static class MeanReducer extends Reducer<Text,IntWritable,Text,IntWritable,IntWritable, IntWritable>{
    private IntWritable meanAge = new IntWritable();
    private IntWritable minAge = new IntWritable();
    private IntWritable maxAge = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int ageSum = 0;
      int count = 0;
      int minAge = 200;
      int maxAge = 0;
      for (IntWritable val : values) {
        value = val.get();
        age_sum += value;
        count++;
        if (value > maxAge){
          maxAge = value;
        }
        if (value < minAge){
          minAge = value;
        }
      }
      meanAge.set(ageSum/count);
      context.write(key, result, minAge, maxAge);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();


    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "player_age");
    job.setJarByClass(PlayerAge.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(MeanReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
