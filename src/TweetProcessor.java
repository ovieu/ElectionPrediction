//package org.group.nine;


import java.io.*;
import java.util.*;
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

public class TweetProcessor {

	private static HashSet<String> positiveWordDictionary = new HashSet<String>();
	private static HashSet<String> negativeWordDictionary = new HashSet<String>();
	private static HashSet<String> hillaryWords= new HashSet<String>();
	private static HashSet<String> trumpWords= new HashSet<String>();

	//constants
	private static final String trumpPositive = "TrumpPositive";
	private static final String trumpNegative = "TrumpNegative";
	private static final String hillaryNegative = "HillaryNegative";
	private static final String hillaryPositive = "HillaryPositive";
	private static final String neutral= "neutral";

        private static ClassLoader classLoader = TweetProcessor.class.getClassLoader();

    //the static initializer is used to create the dictionaries once    
	static {
		try{
			loadCorpus();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void loadCorpus() throws Exception {
    		String word;
                BufferedReader reader= null;

		//load -ve words
                reader= new BufferedReader(
                        new InputStreamReader(  classLoader.getResourceAsStream("wordSet/negativeWords.txt"),"UTF-8")
                );
                while ((word = reader.readLine()) != null) {
                        negativeWordDictionary.add(word.toLowerCase()); //
                }
                reader.close();

		//load +ve words
                reader= new BufferedReader(
                        new InputStreamReader(  classLoader.getResourceAsStream("wordSet/positiveWords.txt"),"UTF-8")
                );
                while ((word = reader.readLine()) != null) {
                        positiveWordDictionary.add(word.toLowerCase());
                }
                reader.close();

		//load trump words
                reader= new BufferedReader(
                        new InputStreamReader(  classLoader.getResourceAsStream("wordSet/trump.txt"),"UTF-8")
                );
                while ((word = reader.readLine()) != null) {
                        trumpWords.add(word.toLowerCase());
                }
                reader.close();

		//load hillary words
                reader= new BufferedReader(
                        new InputStreamReader(  classLoader.getResourceAsStream("wordSet/hillary.txt"),"UTF-8")
                );
                while ((word = reader.readLine()) != null) {
                        hillaryWords.add(word.toLowerCase());
                }
                reader.close();

	}

	//this is where you perform polarity calculation and tweet identification
	public String process(String s){
	
		//default result 	
		String result = neutral;

		//this is where you store the positivewordcount and negative word count
		int[] sentiment = getSentiment(s);
		
		int positiveCount = sentiment[0];
		int negativeCount = sentiment[1];
		
		//neutral cases result in nothing
		//neutral sentiment
		if( negativeCount == positiveCount ){
			//System.out.println("no sentiment "+s);
			return result;
		}

		//check if this is a trump tweet or hillary tweet
		boolean isTrump = false;
		boolean isClinton = false;
		isTrump = isTrump(s);
		isClinton = isClinton(s);

		//neutral cases result in nothing
		//if this tweet contains trump and hillary tweet, ignore and return neutral
		if( isTrump && isClinton ){
			//System.out.println("no leaning "+s);
			return result;
		}


		//if the negative conunt is greater and its a trump tweet return result
		if( negativeCount > positiveCount){
			if( isTrump && !isClinton){
				return trumpNegative;
			}else{
				if( !isTrump && isClinton){
					return hillaryNegative;
				}
			}
		}else{

			if( positiveCount > negativeCount){
				if( isTrump && !isClinton){
					return trumpPositive;
				}else{
					if( !isTrump && isClinton){
						return hillaryPositive;
					}
				}
			}
		}

		return result;
	}

	//checks if input 's' contains a word from the trump bag
	//is trump method to check if the tweet contains trump keyword
	private boolean isTrump(String s){
		s = s.toLowerCase();
		for( String k: trumpWords){
			if( s.contains(k)){
				return true;
			}
		} 
		return false;
	}
			
	private boolean isClinton(String s){
		s = s.toLowerCase();
		for( String k: hillaryWords){
			if( s.contains(k)){
				return true;
			}
		} 
		return false;
	}

	//returns an array of positive 
	private int[] getSentiment(String Tweet){

		int[] result = new int[2];
		result[0]=0;
		result[1]=0;//defaults

		int negativeWordsInTweet = 0;
		int positiveWordsInTweet = 0;
                   
		for(String word : Tweet.split("\\W+")){
			//System.out.println(word);
                        //check if the word is a positive or negative word
                        if(positiveWordDictionary.contains(word)){
				//System.out.println("is +ve");
                            	positiveWordsInTweet++;
                        }else if(negativeWordDictionary.contains(word)){
				//System.out.println("is -ve");
                            	negativeWordsInTweet++;
                        }
		}

		result[0] = positiveWordsInTweet;
		result[1] = negativeWordsInTweet;

		return result;
		
	}
}
