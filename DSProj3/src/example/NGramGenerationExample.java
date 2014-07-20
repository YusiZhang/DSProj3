package example;



import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import mapred.Job;
import node.ClientMain;
import config.ParseConfig;

public class NGramGenerationExample {
	public static void main(String[] args) throws Exception {
		try {
			new ParseConfig(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Job job = new Job("NGramGeneration");
		job.setMapperClass("NGramGenerationMapper");

		job.setReducerClass("NGramGenerationReducer");
		job.setInputFileName(args[1]);
		job.setOutputFileName(args[2]);
		job.setReducerTaskSplits(ParseConfig.ReducerTaskSplits);

		Socket socket;

		socket = new Socket(ParseConfig.MasterIP, ParseConfig.MasterMainPort);
		ClientMain.putFileHandler(socket, job.getInputFileName());
		job.waitForCompletion(args[0]);

	}

}