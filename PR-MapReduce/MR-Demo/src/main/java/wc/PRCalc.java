package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class PRCalc extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PRCalc.class);

	public static enum PR_MASS {
		dangling_mass
	}


	public static final float dampingFactor = 0.85f;
	public static final int k = 1000;
	public static final int v = k*k;

	public static class AdjMapper extends Mapper<Object, Text, Text, Text> {

		private Text vertex = new Text();
		private Text vertexValue = new Text();
		private Text inVertex = new Text();
		private Text inVertexPR = new Text();



		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			String valueKey = itr.nextToken();
			String valueVal = itr.nextToken();
			String[] adjList = valueVal.split(";");
			vertex.set(valueKey);
			vertexValue.set(valueVal);
			context.write(vertex,vertexValue);
			String pr = adjList[1];

			String[] adjString=adjList[0].split("#");
			List<String> adj = new ArrayList<>();
			//long dMass = 0;
			//long dMass = context.getConfiguration().getLong("dummyMass",0);
			Float prNew = Float.parseFloat(pr)/(adjString.length);
			for(String s:adjString) {
				if (!s.isEmpty()) {
					inVertex.set(s);
					inVertexPR.set(prNew.toString());
					context.write(inVertex, inVertexPR);
				}
			}

		}
	}

	public static class AdjReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			logger.info("in the second reducer222");
			List<String> valuesList = new ArrayList<>();
			double danglingMass = 0.0;
			String[] adjList;
			String vertexList="";
			if(Integer.parseInt(key.toString())!=0) {
				//long dMass = context.getCounter(PageRankMain.PR_MASS.dangling_mass).getValue();
				for (Text node : values) {
					valuesList.add(node.toString());
					if (node.toString().contains(";")) {
						adjList = node.toString().split(";");
						vertexList = adjList[0];
					}

				}
				if(valuesList.size()==1)
				{
					Text valueOut = new Text(vertexList+";"+"0");
					logger.info("Page rank value=="+0+" for---"+key);
					context.write(key,valueOut);
				}
				else{
					float sumPR =0.0f;
					for(String s: valuesList)
					{
						if(!s.contains(";")) {
							sumPR += Float.parseFloat(s);
						}

					}

					float PR = ((1-dampingFactor)/v)
							+(dampingFactor*sumPR);
					logger.info("Page rank value=="+PR+" for---"+key);
					Text valueOut = new Text(vertexList+";"+PR);
					context.write(key,valueOut);

				}
			}

			else{
				double totalDM = 0.0;
				for (Text node : values) {
					totalDM+=Double.parseDouble(node.toString());
				}
				danglingMass=totalDM/v;
				long convertdanglingMass = (long)(danglingMass*10000);
				logger.info("dangling mass in 0=="+convertdanglingMass+" for---"+key);
				context.getCounter(PR_MASS.dangling_mass).setValue(convertdanglingMass);
			}
		}
	}

	public static class AdjMapper1 extends Mapper<Object, Text, Text, Text> {

		private Text vertex = new Text();
		private Text vertexValue = new Text();
		private Text inVertex = new Text();
		private Text inVertexPR = new Text();



		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			String valueKey = itr.nextToken();
			String valueVal = itr.nextToken();
			vertex.set(valueKey);
			vertexValue.set(valueVal);
			context.write(vertex,vertexValue);


		}
	}

	public static class AdjReducer1 extends Reducer<Text, Text, Text, Text> {
		public String dMass;
		public double convertedMass;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			dMass = context.getConfiguration().get("PageRankMass.Val");
			logger.info("the danfgling masssss==="+dMass);
			convertedMass = Double.parseDouble(dMass)/10000;
			logger.info("the dangling mass in double=====>"+convertedMass);

			/*
			dMass = context.getConfiguration().getLong("dummyMass.val",0);
			convertedMass = (double) dMass/10000;
			logger.info("the danfgling masssss==="+convertedMass);
*/
		}

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			logger.info("in the second reducer");
			List<String> valuesList = new ArrayList<>();
			float danglingMass = 0.0f;

			for (Text node : values) {
				valuesList.add(node.toString());
			}


				String[] adjPR = valuesList.get(0).split(";");
				String sPR = adjPR[1];
				String vertexList = adjPR[0];

				Double newPR = Double.parseDouble(sPR)+(dampingFactor*convertedMass);
				logger.info("total page rank for---"+key+"PR=="+newPR);
				Text valueOut = new Text(vertexList+";"+newPR.toString());
				context.write(key,valueOut);



		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		String intermeOut = "s3://mapreduce-course/outputInter/inter";

		//String intermeOut = "/home/rachna/cs6240/Assignment4/PR-MapReduce/MR-Demo/finalout/inter";
		// Job1 has a Mapper and a Reducer
		final Job job = Job.getInstance(conf, "Page Rank Calculation");



		job.setJarByClass(PRCalc.class);


		final Configuration jobConf1 = job.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(intermeOut));


		job.setMapperClass(AdjMapper.class);
		job.setReducerClass(AdjReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(intermeOut));

        if(!job.waitForCompletion(true)){
            System.exit(1);
        }

		final Configuration conf1 = getConf();
		// Job2 has a Mapper and a Reducer
		final Job job2 =  Job.getInstance(conf1, "Page rank evaluation");
		job2.setJarByClass(PRCalc.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf2.set("mapreduce.output.textoutputformat.separator2", "\t");
		Counter st 	= job.getCounters().findCounter(PR_MASS.dangling_mass);
		Long dummyMassVal= st.getValue();
		String stName = st.getDisplayName();
		logger.info("in runnn method2222===of "+stName+"with value====>"+String.valueOf(dummyMassVal));

		jobConf2.set("PageRankMass.Val",String.valueOf(dummyMassVal));

		job2.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job2, new Path(intermeOut));
		job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);

		LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));


		job2.setMapperClass(AdjMapper1.class);
		job2.setReducerClass(AdjReducer1.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//job2.setInputFormatClass(TextInputFormat.class);
		//job2.setOutputFormatClass(TextOutputFormat.class);
		//FileInputFormat.addInputPath(job2, new Path(intermeOut));
		//FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		//conf2.addResource(new Path(intermeOut));
		FileSystem fs1 = FileSystem.get(new URI("s3://mapreduce-course"),conf2);
		fs1.delete(new Path(intermeOut));

		return 0;

	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <dangling mass>");
		}

		try {

			ToolRunner.run(new PRCalc(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}