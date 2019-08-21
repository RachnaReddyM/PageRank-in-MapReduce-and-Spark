package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;


public class GraphMaker extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(GraphMaker.class);


	public static class GraphMakerMapper extends Mapper<Object, Text, Text, Text> {

		private Text fromKey = new Text();
		private Text toKey = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {


			String vertices = value.toString();
			Integer k = Integer.parseInt(vertices);
			for(int i=1;i<=k*k;i++) {
			    if(i%k==0)
                {
                    fromKey.set(String.valueOf(i));
                    toKey.set("0");
                }
			    else {
			        fromKey.set(String.valueOf(i));
			        toKey.set(String.valueOf(i+1));

                }
                context.write(fromKey, toKey);
            }



		}
	}


	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		// Job1 has a Mapper and a Reducer
		final Job job1 = Job.getInstance(conf, "Reduce Side Join1");

		job1.setJarByClass(GraphMaker.class);

		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", ",");


		job1.setMapperClass(GraphMakerMapper.class);
		job1.setNumReduceTasks(0);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		// Wait until Job1 is completed


		return job1.waitForCompletion(true)?0:1;
	}



	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {

			int status = ToolRunner.run(new GraphMaker(), args);


		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}