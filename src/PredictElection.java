//package org.group.nine;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.*;


public class PredictElection {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		// 1. this section checks to see if the input and output directories
		//	  were passed to the PredictElection program
		if(args.length != 2) {
			System.err.println("Please check the parameters to the predictElection\n"
							   + "program to ensure it has the correct input and outupt directories");
			 System.out.printf("Usage: PredictElection <input dir>  <output dir>\n");
			      System.exit(-1);
		}
		
		//2. Create new job object
		//	 To accomplish this, identify the jar which contains
		//	 the mapper and reducer 
		Job job = new Job();
		job.setJarByClass(PredictElection.class);
		job.setJobName("Predict Election");
		
		
		//3. Specify the input directory where the job files would be
		//	 read and the output directory where the result would be
		//   submitted to
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		//4. Specify the mapper and reducer class
		job.setMapperClass(PredictElectionMapper.class);
		job.setReducerClass(PredictElectionReducer.class);
	
		//5. The intermediate data type for the output key and values
		//   produced by the mapper.
		//  Tweets --> M( ) --- > {TrumpNegative, 1};
		//  Tweets --> M( ) ----> {HilaryPositive, 1};
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//6. Start the job and wait for it to complete. Parameter
		//	 is Boolean, sepecify verbosity: if true, display progress
		//   to user. Finally, exit with a return code.
		//   if succesful, exit with 
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
		
	
	}

}
