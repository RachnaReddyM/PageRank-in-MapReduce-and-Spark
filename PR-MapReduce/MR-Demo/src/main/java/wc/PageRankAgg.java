package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class PageRankAgg extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PageRankAgg.class);

	public static class PRTotalMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {


			final StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
			String valueKey = itr.nextToken();
			String valueVal = itr.nextToken();
			String[] adjList = valueVal.split(";");

			String pr = adjList[1];
			logger.info(" in the  agg mapper===="+pr);
			context.write(new Text("PR"),new Text(pr));

		}
	}

	public static class PRTotalReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			double finalPR =0.0;
			for(Text node: values)
			{
				finalPR+=Double.parseDouble(node.toString());
			}

			logger.info(" in the  agg reducer===="+finalPR);
			context.write(new Text("final PR"),new Text(String.valueOf(finalPR)));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		// Job1 has a Mapper and a Reducer
		final Job job1 = Job.getInstance(conf, "Reduce Side Join1");

		job1.setJarByClass(PageRankAgg.class);

		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

		job1.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job1, new Path(args[0]));
		job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);

		LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));


		job1.setMapperClass(PRTotalMapper.class);
		job1.setReducerClass(PRTotalReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(job1, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		// Wait until Job1 is completed


		return job1.waitForCompletion(true)?0:1;
	}



	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {

			int status = ToolRunner.run(new PageRankAgg(), args);


		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}