import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class InvertedIndex {
public static class IndexMap extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {
	private final static Text word = new Text(); 
	private final static Text position = new Text(); 

	@Override
	public void map(LongWritable key, Text val,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		position.set(fileName);
		String line  = val.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			output.collect(word, position);	
		}
		
	}

}

public static class IndexReduce  extends MapReduceBase implements Reducer<Text, 
Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> val,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	      boolean first = true;
	      StringBuilder toReturn = new StringBuilder();
	      while (val.hasNext()){
	        if (!first)
	          toReturn.append(",");
	        first=false;
	        toReturn.append(val.next().toString());
	      }

	      output.collect(key, new Text(toReturn.toString()));
		
	}

}
public static void main(String[] args) throws Exception {
	JobClient client = new JobClient();
    JobConf conf = new JobConf(InvertedIndex.class);

    conf.setJobName("InvertedIndexer");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(conf, new Path("input"));
    FileOutputFormat.setOutputPath(conf, new Path("output"));

    conf.setMapperClass(IndexMap.class);
    conf.setReducerClass(IndexReduce.class);

    JobClient.runJob(conf);
}

}
