package example;

import java.net.Socket;

import node.ClientMain;
import config.ParseConfig;
import mapred.JobConf;
import mapred.Job;
//import WordCount.Map;
//import WordCount.Reduce;

public class WordCountExample {

	 public static void main(String[] args) throws Exception {
	    JobConf conf = new JobConf();
//	    Job job = new Job(conf, "wordcount");
	   
	    Job job = new Job("wordcount");
//	    job.setOutputKeyClass(Text.class);
//	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass("WordCountMap");
	    job.setReducerClass("WordCountReduce");
	        
//	    job.setInputFormatClass(TextInputFormat.class);
//	    job.setOutputFormatClass(TextOutputFormat.class);
	        
//	    job.setInputPath(args[1]);
//	    job.setOutputPath(args[2]);
	    
	    job.setInputFileName(args[1]);
	    job.setOutputFileName(args[2]);
	    /*copy file from path to dfs*/
	    Socket socket = new Socket(ParseConfig.MasterIP, ParseConfig.MasterMainPort);
	    ClientMain.putFileHandler(socket, job.getInputFileName());
	    
	    job.waitForCompletion(args[0]);
	 }
	       
}
