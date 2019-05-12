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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

  public static class SortMapper extends Mapper<IntWritable, Node, DoubleWritable, IntWritable> {
    public void map(IntWritable nodeId, Node node, Context context) throws IOException, InterruptedException {
      context.write(new DoubleWritable(node.pageRank), nodeId);
    }
  }

  public static class SortReducer extends Reducer<DoubleWritable, IntWritable, IntWritable, Text> {
    public void reduce(DoubleWritable pageRank, Iterable<IntWritable> nodeIds, Context context)
    throws IOException, InterruptedException {
      for (IntWritable nodeId: nodeIds) {
        context.write(nodeId, new Text(String.format("%.9f", pageRank.get())));
      }
    }
  }

  // Source: https://stackoverflow.com/questions/11670953/reverse-sorting-reducer-keys
  public static class ReverseSortComparator extends WritableComparator {
    protected ReverseSortComparator() {
      super(IntWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return -1 * ((IntWritable) a).compareTo((IntWritable) b);
    }
  }

  public static class NamesMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    IntWritable id = new IntWritable();
    Text name = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parsed = value.toString().split(" ", 2);
      id.set(Integer.parseInt(parsed[0]));
      name.set("name_" + parsed[1]);
      context.write(id, name);
    }
  }

  public static class PageRanksMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    IntWritable id = new IntWritable();
    Text pageRank = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parsed = value.toString().split("	", 2);
      id.set(Integer.parseInt(parsed[0]));
      pageRank.set("pr_" + parsed[1]);
      context.write(id, pageRank);
    }
  }

  public static class NamesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable nodeId, Iterable<Text> pr_or_names, Context context)
    throws IOException, InterruptedException {
      String result = "";
      for (Text pr_or_name: pr_or_names) {
        if (pr_or_name.toString().startsWith("pr_")) {
          result += pr_or_name.toString().replace("pr_", "");
        } else if (pr_or_name.toString().startsWith("name_")) {
          result = pr_or_name.toString().replace("name_", "") + " " + result;
        }
      }
      context.write(nodeId, new Text(result));
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

    // Sorting step:
    // Sorts nodes by their page rank
    Configuration sortingConf = new Configuration();
    Job sorting = Job.getInstance(sortingConf, "sorting");
    sorting.setJarByClass(PageRank.class);
    sorting.setMapperClass(SortMapper.class);
    sorting.setReducerClass(SortReducer.class);
    sorting.setInputFormatClass(SequenceFileInputFormat.class);
    sorting.setMapOutputKeyClass(DoubleWritable.class);
    sorting.setMapOutputValueClass(IntWritable.class);
    sorting.setOutputKeyClass(IntWritable.class);
    sorting.setOutputValueClass(Text.class);
    sorting.setSortComparatorClass(ReverseSortComparator.class);
    FileInputFormat.addInputPath(sorting, intermediateOutputPath);
    intermediateOutputPath = new Path(intermediateFolder + "_" + (numIterations + 1));
    FileOutputFormat.setOutputPath(sorting, intermediateOutputPath);
    sorting.waitForCompletion(true);

    // Naming step:
    // Adds names to the result and outputs the node id, its name and its pagerank
    Configuration namingConf = new Configuration();
    Job naming = Job.getInstance(namingConf, "naming");
    naming.setJarByClass(PageRank.class);
    naming.setReducerClass(NamesReducer.class);
    naming.setMapOutputKeyClass(IntWritable.class);
    naming.setMapOutputValueClass(Text.class);
    naming.setOutputKeyClass(IntWritable.class);
    naming.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(naming, intermediateOutputPath, TextInputFormat.class, PageRanksMapper.class);
    MultipleInputs.addInputPath(naming, namesInputPath, TextInputFormat.class, NamesMapper.class);
    FileOutputFormat.setOutputPath(naming, outputPath);
    naming.waitForCompletion(true);
    System.exit(0);
  }
}
