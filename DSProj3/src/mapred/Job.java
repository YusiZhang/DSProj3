package mapred;

import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;

import node.Scheduler;
import communication.Message;
import config.ParseConfig;

public class Job implements Serializable{
	protected int jobId;
	protected String jobName;
	protected String MapperClass;
	protected String ReducerClass;
	protected Class<?> inputFormatClass;
	protected Class<?> outputFormatClass;
	protected String inputFileName;
	protected String outputFileName;
	protected String inputPath;
	protected String outputPath;
	protected Message msg;
	private int taskSplits;
	private int finishedTasks;

	public Job(){}
	public Job(String jobName) {
		this.jobName = jobName;
	}
	public int getJobId() {
		return jobId;
	}
	public void setJobId() {
		this.jobId = Scheduler.curJobId;
		Scheduler.curJobId++;
	}
	
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public String getMapperClass() {
		return MapperClass;
	}
	public void setMapperClass(String mapperClass) {
		MapperClass = mapperClass;
	}
	public String getReducerClass() {
		return ReducerClass;
	}
	public void setReducerClass(String reducerClass) {
		ReducerClass = reducerClass;
	}
	public Class<?> getInputFormatClass() {
		return inputFormatClass;
	}
	public void setInputFormatClass(Class<?> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}
	public Class<?> getOutputFormatClass() {
		return outputFormatClass;
	}
	public void setOutputFormatClass(Class<?> outputFormatClass) {
		this.outputFormatClass = outputFormatClass;
	}

	//client calls waitForCompletion to run the job
	public void waitForCompletion(String config) {
		ParseConfig conf;
		try {
			conf = new ParseConfig(config);
			//connect to master
			Socket socket = new Socket(conf.MasterIP, conf.MasterMainPort);
			msg = new Message(Message.MSG_TYPE.NEW_JOB,this);
			msg.send(socket);
			
			msg = Message.receive(socket);
			handleMsgFromMaster(msg);
			//closes the socket
			socket.close();
			
		} catch (Exception e) {
			System.out.println("fail to submit the job to master!");
			e.printStackTrace();
		}
	}
	
	public void handleMsgFromMaster(Message msg) {
		switch(msg.getType()){
			case JOB_COMP:
				System.out.println("Job "+jobName+"completed sucessfully!");
				break;
				
			case JOB_FAIL:
				System.out.println("Job "+jobName+"is killed by the master!");
				break;
			
			default:
				break;
			
		}
	}
	public String getInputFileName() {
		return inputFileName;
	}
	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}
	public String getOutputFileName() {
		return outputFileName;
	}
	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}
	public String getInputPath() {
		return inputPath;
	}
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	public String getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	public int getFinishedTasks() {
		return finishedTasks;
	}
	public void setFinishedTasks(int finishedTasks) {
		this.finishedTasks = finishedTasks;
	}
	public int getTaskSplits() {
		return taskSplits;
	}
	public void setTaskSplits(int taskSplits) {
		this.taskSplits = taskSplits;
	}
	
	
}
