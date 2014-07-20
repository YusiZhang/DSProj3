package example;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import node.ClientMain;
import config.ParseConfig;
import mapred.JobConf;
import mapred.Job;
//import WordCount.Map;
//import WordCount.Reduce;

public class WordCountExample {

	 public static void main(String[] args) {
		 /*args[0]: src/ConfigFile.txt 	arg[1]: input file name args[2]: output file name*/
//	    JobConf conf = new JobConf();
//	    Job job = new Job(conf, "wordcount");
		try {
			new ParseConfig(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
	    Job job = new Job("wordcount");
//	    job.setOutputKeyClass(Text.class);
//	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass("WordCountMaper");
	    job.setReducerClass("WordCountReducer");
	        
//	    job.setInputFormatClass(TextInputFormat.class);
//	    job.setOutputFormatClass(TextOutputFormat.class);
	        
//	    job.setInputPath(args[1]);
//	    job.setOutputPath(args[2]);
	    
	    job.setInputFileName(args[1]);
	    job.setOutputFileName(args[2]);
	    job.setReducerTaskSplits(ParseConfig.ReducerTaskSplits);
	    /*copy file from path to dfs*/
	    Socket socket;
		try {
			socket = new Socket(ParseConfig.MasterIP, ParseConfig.MasterMainPort);
			ClientMain.putFileHandler(socket, job.getInputFileName());
			job.waitForCompletion(args[0]);
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			try {
//				socket = new Socket(ParseConfig.MasterIP, ParseConfig.MasterMainPort);
//				ClientMain.putFileHandler(socket, job.getInputFileName());
//				job.waitForCompletion(args[0]);
//			} catch (UnknownHostException e1) {
//				e1.printStackTrace();
//			} catch (IOException e1) {
//				e1.printStackTrace();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			
//			e.printStackTrace();
//		}
	    
	    
	

	 }
	       
}
