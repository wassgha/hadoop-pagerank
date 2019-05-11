import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DeadEnds {
  public static class OutLinkMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable nodeId = new IntWritable();
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parsed = value.toString().split(" ");
      // Origin node (1 out link)
      nodeId.set(Integer.parseInt(parsed[0]));
      context.write(nodeId, one);
      // Destination node (0 out link)
      nodeId.set(Integer.parseInt(parsed[1]));
      context.write(nodeId, zero);
    }
  }

  public static class OutDegreeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable outDegree = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      outDegree.set(sum);
      context.write(key, outDegree);
    }
  }

  public static class NullOutDegreeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable outDegree = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (sum == 0) {
        outDegree.set(sum);
        context.write(key, outDegree);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    boolean USE_EXCERPT = false;
    String NODES_FILE_NAME  = "wiki-topcats" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
    String NAMES_FILE_NAME  = "wiki-topcats-page-names" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
    String CATS_FILE_NAME   = "wiki-topcats-categories" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";

    Path nodesInputPath   = new Path(args[0] + '/' + NODES_FILE_NAME);
    Path namesInputPath   = new Path(args[0] + '/' + NAMES_FILE_NAME);
    Path outputPath       = new Path(args[1]);

    // First mapper + reducer (emits nodes and their out-link count)
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "deadends");
    job1.setJarByClass(DeadEnds.class);
    job1.setMapperClass(OutLinkMapper.class);
    job1.setReducerClass(NullOutDegreeReducer.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, nodesInputPath);
    FileOutputFormat.setOutputPath(job1, outputPath);
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}
