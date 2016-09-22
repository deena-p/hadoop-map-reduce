package com.hadoop.deena.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class WCFinder extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {

		if (arg0.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Job job = new Job(getConf(), "MyWordCount");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
	
		return job.waitForCompletion(true)?0:1;
	}


	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WCFinder(), args);
		System.exit(exitCode);
	}
}
