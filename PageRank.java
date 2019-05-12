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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PageRank {
  // Constants
  final static boolean  USE_EXCERPT       = false;
  final static String   NODES_FILE_NAME   = "wiki-topcats" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
  final static String   NAMES_FILE_NAME   = "wiki-topcats-page-names" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
  final static String   CATS_FILE_NAME    = "wiki-topcats-categories" + (USE_EXCERPT ? ".excerpt" : "") + ".txt";
  final static double   NUMBER_OF_NODES   = 1791489;

  public static class Node implements Writable {
    public Integer id;
    public Double pageRank;
    public ArrayList<Integer> adjacencyList;

    public Node() {
      this.id = null;
      this.pageRank = null;
      this.adjacencyList = new ArrayList<Integer>();
    }

    public Node(Integer id, Double pageRank) {
        this.id = id;
        this.pageRank = pageRank;
        this.adjacencyList = new ArrayList<Integer>();
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setPageRank(Double pageRank) {
        this.pageRank = pageRank;
    }

    public void addAdjacent(Integer id) {
        this.adjacencyList.add(id);
    }

    public boolean isNode() {
      return this.id != null;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      boolean isNode = in.readBoolean();
      if (isNode) {
        this.id = in.readInt();
        this.pageRank = in.readDouble();
        int adjacencyListSize = in.readInt();
        this.adjacencyList = new ArrayList<Integer>(adjacencyListSize);
        for (int i = 0; i < adjacencyListSize; i++) {
          this.adjacencyList.add(in.readInt());
        }
      } else {
        this.id = null;
        this.pageRank = in.readDouble();
        this.adjacencyList = new ArrayList<Integer>();
      }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeBoolean(this.isNode());
      if (this.isNode()) {
        out.writeInt(this.id);
        out.writeDouble(this.pageRank);
        out.writeInt(this.adjacencyList.size());
        for (Integer adjacent : this.adjacencyList) {
          out.writeInt(adjacent);
        }
      } else {
        out.writeDouble(this.pageRank);
      }
    }

    @Override
    public String toString()
    {
      String neighbors = "[";
      for (Integer adjacent: adjacencyList) {
        neighbors += adjacent + ", ";
      }
      neighbors += "]";
      return "Node <id: " + this.id + " pageRank: " + this.pageRank + " neighbors: " + neighbors + ">";
    }

    public static Node read(DataInput in) throws IOException {
        Node node = new Node();
        node.readFields(in);
        return node;
    }

    public static Node clone(Node toClone) {
      Node node = new Node(toClone.id, toClone.pageRank);
      for (Integer adjacent: toClone.adjacencyList) {
        node.addAdjacent(adjacent);
      }
      return node;
    }
  }

  public static class IntializationMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable srcId = new IntWritable();
    private IntWritable dstId = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parsed = value.toString().split(" ");
      // Source node
      srcId.set(Integer.parseInt(parsed[0]));
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
      n.setPageRank(((double) 1.0)/NUMBER_OF_NODES);
      for (IntWritable val : values) {
        n.addAdjacent(val.get());
      }
      context.write(key, n);
    }
  }

  public static class PageRankIterMapper extends Mapper<IntWritable, Node, IntWritable, Node> {
    public void map(IntWritable nodeId, Node node, Context context) throws IOException, InterruptedException {
      double pageRankFraction = node.pageRank/((double) node.adjacencyList.size());
      context.write(nodeId, node);
      for (Integer adjacent: node.adjacencyList) {
        Node placeholder = new Node();
        placeholder.setPageRank(pageRankFraction);
        context.write(new IntWritable(adjacent), placeholder);
      }
    }
  }

  public static class PageRankIterReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    public void reduce(IntWritable nodeId, Iterable<Node> pageRanksOrNode, Context context)
    throws IOException, InterruptedException {
      Node m = null;
      double pageRank = 0.0;
      for (Node pageRankOrNode:pageRanksOrNode) {
        if (pageRankOrNode.isNode()) {
          m = Node.clone(pageRankOrNode);
        } else {
          pageRank += pageRankOrNode.pageRank;
        }
      }
      if (m != null) {
        m.setPageRank(pageRank);
        context.write(nodeId, m);
      }
    }
  }

  public static class ResultMapper extends Mapper<IntWritable, Node, DoubleWritable, IntWritable> {
    public void map(IntWritable nodeId, Node node, Context context) throws IOException, InterruptedException {
      context.write(new DoubleWritable(node.pageRank), nodeId);
    }
  }

  public static class ResultReducer extends Reducer<DoubleWritable, IntWritable, IntWritable, DoubleWritable> {
    public void reduce(DoubleWritable pageRank, Iterable<IntWritable> nodeIds, Context context)
    throws IOException, InterruptedException {
      for (IntWritable nodeId: nodeIds) {
        context.write(nodeId, pageRank);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int numIterations           = Integer.parseInt(args[0]);
    Path nodesInputPath         = new Path(args[1] + '/' + NODES_FILE_NAME);
    Path namesInputPath         = new Path(args[1] + '/' + NAMES_FILE_NAME);
    String intermediateFolder   = args[2];
    Path intermediateOutputPath = new Path(intermediateFolder + "_0");
    Path outputPath             = new Path(args[3]);

    // Initialization step: 
    // Emits nodes and their adjacency lists with an initial pagerank of 1/n
    Configuration intializeConf = new Configuration();
    Job intialize = Job.getInstance(intializeConf, "intialize");
    intialize.setJarByClass(PageRank.class);
    intialize.setMapperClass(IntializationMapper.class);
    intialize.setReducerClass(InitializationReducer.class);
    intialize.setMapOutputKeyClass(IntWritable.class);
    intialize.setMapOutputValueClass(IntWritable.class);
    intialize.setOutputKeyClass(IntWritable.class);
    intialize.setOutputValueClass(Node.class);
    intialize.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(intialize, nodesInputPath);
    SequenceFileOutputFormat.setOutputPath(intialize, intermediateOutputPath);
    intialize.waitForCompletion(true);

    // 10 Iterative steps:
    // Re-calculate the page rank for each node
    for (int i = 0; i < numIterations; i++) {
      Configuration iterationConf = new Configuration();
      Job iteration = Job.getInstance(iterationConf, "iteration_" + i);
      iteration.setJarByClass(PageRank.class);
      iteration.setMapperClass(PageRankIterMapper.class);
      iteration.setReducerClass(PageRankIterReducer.class);
      iteration.setInputFormatClass(SequenceFileInputFormat.class);
      iteration.setOutputKeyClass(IntWritable.class);
      iteration.setOutputValueClass(Node.class);
      iteration.setOutputFormatClass(SequenceFileOutputFormat.class);
      FileInputFormat.addInputPath(iteration, intermediateOutputPath);
      intermediateOutputPath = new Path(intermediateFolder + "_" + (i + 1));
      SequenceFileOutputFormat.setOutputPath(iteration, intermediateOutputPath);
      iteration.waitForCompletion(true);
    }

    // Adding page names and sorting:
    // Sorts nodes by their page rank and outputs their name and page rank in text
    Configuration resultConf = new Configuration();
    Job result = Job.getInstance(resultConf, "result");
    result.setJarByClass(PageRank.class);
    result.setMapperClass(ResultMapper.class);
    result.setReducerClass(ResultReducer.class);
    result.setInputFormatClass(SequenceFileInputFormat.class);
    result.setMapOutputKeyClass(DoubleWritable.class);
    result.setMapOutputValueClass(IntWritable.class);
    result.setOutputKeyClass(IntWritable.class);
    result.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(result, intermediateOutputPath);
    FileOutputFormat.setOutputPath(result, outputPath);
    result.waitForCompletion(true);
    System.exit(0);
  }
}
