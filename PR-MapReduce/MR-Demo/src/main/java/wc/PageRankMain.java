package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import wc.PageRank;
import wc.PRCalc;


import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class PageRankMain  {

	private static final Logger logger = LogManager.getLogger(PageRankMain.class);

	public static int status;

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {

            String adjOutput = "s3://mapreduce-course/outputAdj";
            String pageRankCalc = "s3://mapreduce-course/outputFinal";

			//String adjOutput = "/home/rachna/cs6240/Assignment4/PR-MapReduce/MR-Demo/adjOutput";
			//String pageRankCalc = "/home/rachna/cs6240/Assignment4/PR-MapReduce/MR-Demo/finalOutput";

			status = ToolRunner.run(new GraphMaker(), args);
            status = ToolRunner.run(new PageRank(), new String[]{args[1],adjOutput});

			String output = "s3://mapreduce-course/outputItr";

			//String output = "/home/rachna/cs6240/Assignment4/PR-MapReduce/MR-Demo/itrOutput";

			String interOutput = "";
			String writeTo ="";

			for (int i = 1; i <= 10; i++) {

				if (i == 1) {
				    //interOutput = args[1];
					interOutput = adjOutput;
					writeTo = output + "/iteration1";
				} else {
					interOutput = output + "/iteration" + (i - 1);
					writeTo = output + "/iteration" + i;

				}
				ToolRunner.run(new PRCalc(), new String[]{interOutput, writeTo});

				Configuration confFile = new Configuration();
				FileSystem fs = FileSystem.get(new URI("s3://mapreduce-course"),confFile);
				fs.delete(new Path(interOutput));


			}

			logger.info("input path is====="+writeTo);
            ToolRunner.run(new PageRankAgg(), new String[]{writeTo,pageRankCalc});




        } catch (final Exception e) {
			logger.error("", e);
		}
	}
}