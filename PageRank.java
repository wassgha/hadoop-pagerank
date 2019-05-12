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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PageRank {
  // Constants
  final static boolean  USE_EXCERPT       = true;
  final static String   NODES_FILE_NAME   = "wiki-topcats" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
  final static String   NAMES_FILE_NAME   = "wiki-topcats-page-names" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
  final static String   CATS_FILE_NAME    = "wiki-topcats-categories" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
  final static double   NUMBER_OF_NODES   = 1791489;

  public static class Node implements Writable {
    public int     id;
    public double  pageRank;
    public ArrayList<Integer> adjacencyList;
    
    public Node() {
      this.adjacencyList = new ArrayList<Integer>();
    }

    public Node(int id, int pageRank) {
      this.id = id;
      this.pageRank = pageRank;
      this.adjacencyList = new ArrayList<Integer>();
    }

    public void setId(int id) {
      this.id = id;
    }

    public void setPageRank(double pageRank) {
      this.pageRank = pageRank;
    }

    public void addAdjacent(int id) {
      this.adjacencyList.add(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      this.id = in.readInt();
      this.pageRank = in.readDouble();
      int adjacencyListSize = in.readInt();
      this.adjacencyList = new ArrayList<Integer>(adjacencyListSize);
      for (int i = 0; i < adjacencyListSize; i++) {
        this.adjacencyList.add(in.readInt());
      }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeInt(this.id);
      out.writeDouble(this.pageRank);
      out.writeInt(this.adjacencyList.size());
      for (Integer adjacent : this.adjacencyList) {
        out.writeInt(adjacent);
      }
    }

    public static Node read(DataInput in) throws IOException {
      Node node = new Node();
      node.readFields(in);
      return node;
    }
  }

  public static class IntializationMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable srcId = new IntWritable();
    private IntWritable dstId = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parsed = value.toString().split(" ");
      // Source node
      srcId.set(Integer.parseInt(parsed[1]));
      // Destination node
      dstId.set(Integer.parseInt(parsed[1]));
      // Output <SrcNode, DstNode>
      context.write(srcId, dstId);
    }
  }

  public static class InitializationReducer extends Reducer<IntWritable, IntWritable, IntWritable, Node> {
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      Node n = new Node();
      n.setId(key.get());
      n.setPageRank(1/NUMBER_OF_NODES);
      for (IntWritable val : values) {
        n.addAdjacent(val.get());
      }
      context.write(key, n);
    }
  }

  public static void main(String[] args) throws Exception {
    Path nodesInputPath   = new Path(args[0] + '/' + NODES_FILE_NAME);
    Path namesInputPath   = new Path(args[0] + '/' + NAMES_FILE_NAME);
    Path outputPath       = new Path(args[1]);

    // First mapper + reducer (emits nodes and their out-links)
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "initialization");
    job1.setJarByClass(PageRank.class);
    job1.setMapperClass(IntializationMapper.class);
    job1.setReducerClass(InitializationReducer.class);
    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(Node.class);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    // MultipleInputs.addInputPath(job1, nodesInputPath, AdjencyListMapper.class, AdjencyListMapper.class);
    // MultipleInputs.addInputPath(job1, sencondPath, SecondInputFormat.class, SecondMap.class);
    FileInputFormat.addInputPath(job1, nodesInputPath);
    SequenceFileOutputFormat.setOutputPath(job1, outputPath);
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}
