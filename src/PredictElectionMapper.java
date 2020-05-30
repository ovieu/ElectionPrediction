//package org.group.nine;


import java.io.*;
import java.util.HashSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class PredictElectionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//this is a mapreduce context
	//
	@Override
	public void map(LongWritable key, Text value, Context context) //this is where the tweet comes
		throws IOException, InterruptedException {
		
		TweetProcessor  processor = new TweetProcessor();
		//this is the result from the map process
		//value is te tweet
		//the processor method takes the tweet as a param
		//*the logic runs in process method not in the mapper*//
		String result = processor.process(value.toString());
                        
		context.write(new Text(result), new IntWritable(1));
	}
}
