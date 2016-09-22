package com.hadoop.deena.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//super.map(key, value, context);

		StringTokenizer token = new StringTokenizer(value.toString());

		while (token.hasMoreTokens()){
			context.write(new Text(token.nextToken()), new IntWritable(1));
		}
	}

}
