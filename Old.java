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

public class ItemFrequency {
  public static class ProductPair implements Writable {
    private IntWritable first_id;
    private IntWritable second_id;
    private Text first_name;
    private Text second_name;

    public ProductPair(int first_id, int second_id) {
      this.first_id = new IntWritable(first_id);
      this.second_id = new IntWritable(second_id);
      this.first_name = new Text();
      this.second_name = new Text();
    }

    public ProductPair() {
      this.first_id = new IntWritable(0);
      this.second_id = new IntWritable(0);
      this.first_name = new Text();
      this.second_name = new Text();
    }

    public Text getFirstName() {
      return this.first_name;
    }

    public Text getSecondName() {
      return this.second_name;
    }

    public IntWritable getFirstId() {
      return this.first_id;
    }

    public IntWritable getSecondId() {
      return this.second_id;
    }

    public void setFirstName(String name) {
      this.first_name.set(name);
    }

    public void setSecondName(String name) {
      this.second_name.set(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      this.first_id.readFields(in);
      this.second_id.readFields(in);
      this.first_name.readFields(in);
      this.second_name.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      this.first_id.write(out);
      this.second_id.write(out);
      this.first_name.write(out);
      this.second_name.write(out);
    }

    @Override
    public String toString() {
      String first = this.first_id.toString() + "," + this.first_name.toString();
      String second = this.second_id.toString() + "," + this.second_name.toString();
      return first + "," + second;
      // return this.first_id.compareTo(this.second_id) <= 0 ? first + "," + second : second + "," + first;
    }
  }

  public static class OrderMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable orderId = new IntWritable();
    private IntWritable productId = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      if (key.get() == 0)
      return;
      String[] list = value.toString().split(",");
      orderId.set(Integer.parseInt(list[0]));
      productId.set(Integer.parseInt(list[1]));
      context.write(orderId, productId);
    }
  }

  public static class ProductReducer extends Reducer<IntWritable, IntWritable, ProductPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      ArrayList<IntWritable> cache = new ArrayList<IntWritable>();
      for (IntWritable val : values) {
        cache.add(new IntWritable(val.get()));
      }

      // ListIterator<IntWritable> iterator = cache.listIterator();
      // while (iterator.hasNext()) {
      //   IntWritable val1 = iterator.next();
      //   String id1 = val1.toString();
      //   ListIterator<IntWritable> iterator2 = cache.listIterator(iterator.nextIndex());
      //   while (iterator2.hasNext()) {
      //     IntWritable val2 = iterator2.next();
      //     String id2 = val2.toString();
      //     compositeKey.set(val1.compareTo(val2) <= 0 ? id1 + ',' + id2 : id2 + ',' + id1);
      //     context.write(compositeKey, one);
      //   }
      // }
      for (int i = 0; i < cache.size(); i++) {
        int id1 = cache.get(i).get();
        for (int j = i + 1; j < cache.size(); j++) {
          int id2 = cache.get(j).get();
          ProductPair compositeKey = id1 <= id2 ? new ProductPair(id1, id2) : new ProductPair(id2, id1);
          // compositeKey.set(cache.get(i).compareTo(cache.get(j)) <= 0 ? id1 + ',' + id2 : id2 + ',' + id1);
          context.write(compositeKey, one);
        }
      }
    }
  }

  public static class IntermediateMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text inputKey = new Text();
    private IntWritable inputValue = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] list = value.toString().split("\\s");
      inputKey.set(list[0]);
      inputValue.set(Integer.parseInt(list[1]));
      context.write(inputKey, inputValue);
    }
  }

  // public static class ProductNameMapper extends Mapper<LongWritable, Text, Text, Text> {
  //
  //   private Text productId = new Text();
  //   private Text productName = new Text();
  //
  //   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  //     if (key.get() == 0)
  //       return;
  //     String[] list = value.toString().split(",");
  //     productId.set("prdctName," + list[0]);
  //     productName.set(list[1]);
  //     context.write(productId, productName);
  //   }
  // }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class InversionMapper extends Mapper<Object, Text, IntWritable, Text> {

    private Text inputKey = new Text();
    private IntWritable inputValue = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] list = value.toString().split("\\s");
      inputKey.set(list[0]);
      inputValue.set(Integer.parseInt(list[1]));

      context.write(inputValue, inputKey);
    }
  }

  public static class InversionReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private int count = 0;
    private static final int LIMIT = 100;

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
      if (count >= LIMIT)
      return;
      for (Text val : values) {
        context.write(val, key);
        count++;
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

  public static void main(String[] args) throws Exception {
    String ORDER_FILE_NAME = "order_products__train.csv";
    String PRODUCT_FILE_NAME = "products.csv";

    Path ordersInputPath = new Path(args[0] + '/' + ORDER_FILE_NAME);
    Path productNameInputPath = new Path(args[0] + '/' + PRODUCT_FILE_NAME);
    Path intermediate1Path = new Path(args[1] + "_1");
    Path intermediate2Path = new Path(args[1] + "_2");
    Path outputPath = new Path(args[2]);

    // First mapper + reducer (emits product pairs from orders)
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "orders");
    job1.setJarByClass(ItemFrequency.class);
    job1.setMapperClass(OrderMapper.class);
    // job1.setCombinerClass(CountReducer.class);
    job1.setReducerClass(ProductReducer.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, ordersInputPath);
    FileOutputFormat.setOutputPath(job1, intermediate1Path);
    job1.waitForCompletion(true);

    // Second mapper + reducer (Counts the product pairs bought together)
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "products");
    job2.setJarByClass(ItemFrequency.class);
    job2.setMapperClass(IntermediateMapper.class);
    // job2.setCombinerClass(CountReducer.class);
    // MultipleInputs.addInputPath(job2, intermediatePath,TextInputFormat.class, IntermediateMapper.class);
    // MultipleInputs.addInputPath(job2, productNameInputPath,TextInputFormat.class, ProductNameMapper.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, intermediate1Path);
    FileOutputFormat.setOutputPath(job2, intermediate2Path);
    job2.waitForCompletion(true);

    // Third mapper + reducer (Sorts and Outputs the first 100 entries)
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "sortncrop");
    job3.setJarByClass(ItemFrequency.class);
    job3.setMapperClass(InversionMapper.class);
    job3.setReducerClass(InversionReducer.class);
    job3.setMapOutputKeyClass(IntWritable.class);
    job3.setMapOutputValueClass(Text.class);
    job3.setSortComparatorClass(ReverseSortComparator.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job3, intermediate2Path);
    FileOutputFormat.setOutputPath(job3, outputPath);
    job3.waitForCompletion(true);
    System.exit(job3.waitForCompletion(true) ? 0 : 1);

    // Fourth mapper + reducer (Adds product names)
    // Configuration conf4 = new Configuration();
    // Job job4 = Job.getInstance(conf4, "productnames");
    // job4.setJarByClass(ItemFrequency.class);
    // job4.setMapperClass(InversionMapper.class);
    // job4.setReducerClass(InversionReducer.class);
    // job4.setMapOutputKeyClass(IntWritable.class);
    // job4.setMapOutputValueClass(Text.class);
    // job4.setSortComparatorClass(ReverseSortComparator.class);
    // job4.setOutputKeyClass(Text.class);
    // job4.setOutputValueClass(IntWritable.class);
    // FileInputFormat.addInputPath(job4, intermediate2Path);
    // FileOutputFormat.setOutputPath(job4, outputPath);

    // System.exit(job4.waitForCompletion(true) ? 0 : 1);
  }
}
