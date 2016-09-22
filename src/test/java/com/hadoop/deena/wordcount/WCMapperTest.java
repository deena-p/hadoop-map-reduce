package com.hadoop.deena.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class WCMapperTest {
	  	       
	
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setup(){

		WCMapper mapper = new WCMapper();
		WCReducer reducer = new WCReducer();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>(mapper);
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
	}
	
	@Ignore
	@Test
	public void testWCMapper() throws IOException{
		//mapDriver.withInput(new LongWritable(), new Text("Sachin is good, Sachin is great. Saurav is awesome"));
		mapDriver.withInput(new LongWritable(), new Text("Sachin is Sachin"));
		//mapDriver.withInput(new LongWritable(), new Text("Sachin"));
		mapDriver.withOutput(new Text("Sachin"), new IntWritable(1));
		mapDriver.withOutput(new Text("is"), new IntWritable(1));
		mapDriver.withOutput(new Text("Sachin"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Ignore
	@Test
	public void testWCReducer() throws IOException{
		
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		reduceDriver.addInput(new Text("Sachin"), values );
		reduceDriver.withOutput(new Text("Sachin"), new IntWritable(3));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException{
		mapReduceDriver.withInput(new LongWritable(), new Text("Sachin is Sachin"));
		mapReduceDriver.withOutput(new Text("Sachin"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("is"), new IntWritable(1));
		mapReduceDriver.runTest();
	}
	

}
