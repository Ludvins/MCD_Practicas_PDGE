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
          age.set(Integer.parseInt(split[2]));
          context.write(team, age);
      }
  }

  public static class MeanReducer extends Reducer<Text,IntWritable,Text,Iterable<IntWritable> >{
    private IntWritable meanAge = new IntWritable();
    private IntWritable minAge = new IntWritable();
    private IntWritable maxAge = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int ageSum = 0;
      int count = 0;
      int _minAge = 200;
      int _maxAge = 0;

      for (IntWritable val : values) {
        int value = val.get();
        ageSum += value;
        count++;
        if (value > _maxAge){
          _maxAge = value;
        }
        if (value < _minAge){
          _minAge = value;
        }
      }
      meanAge.set(ageSum/count);
      minAge.set(_minAge);
      maxAge.set(_maxAge);

      // Create a list of IntWritable as output
      IntWritable result[] = {meanAge, minAge, maxAge};
      context.write(key, Arrays.asList(result));
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
