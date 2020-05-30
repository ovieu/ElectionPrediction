//package org.group.nine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictElectionReducer extends Reducer<Text, IntWritable,
													Text, IntWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		
		//set a variable to perform polarity count
		int polarityCount = 0;
		
		//sum all similar polarity value
		//	Example
		//	{trumpPositive, 1}
		//	{trumpPositive, 1} -------> {trumpPositive, 2}
		for (IntWritable value : values) {
			polarityCount += value.get();
		}
		
		//print out the final value to a file in hdfs
		context.write(key, new IntWritable(polarityCount));
	}

}
