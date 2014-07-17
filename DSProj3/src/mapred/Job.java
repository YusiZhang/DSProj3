package mapred;

import io.*;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import node.Scheduler;
import node.SlaveInfo;
import communication.Message;
import communication.ReducerDoneMsg;
import config.ParseConfig;
import debug.Printer;

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
	private InetAddress address;
	private int port;
	private int mapperTaskSplits;
	private int reducerTaskSplits;
	public int finishedMapperTasks = 0;
	public int finishedReducerTasks = 0;
	public int curTaskId = 0;
//	private ArrayList<String> ReducerOutputFile = new ArrayList<String>();
	private ArrayList<SlaveInfo> reduceLists  = new ArrayList<SlaveInfo>();
	public HashMap<Text, FixValue> reduceOutputMap = new HashMap<Text, FixValue>();
	
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
			System.out.println(this.getMapperClass());
			
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
				
				//write file to local
//				HashMap<Text, FixValue> result = (HashMap<Text, FixValue>) msg.getContent();
//				Printer.printT(result);
				ArrayList<String> resultFiles = (ArrayList<String>) msg.getContent();
				Printer.printC(resultFiles);
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
	public int getMapperTaskSplits() {
		return mapperTaskSplits;
	}
	public void setMapperTaskSplits(int mapperTaskSplits) {
		this.mapperTaskSplits = mapperTaskSplits;
	}
	public int getFinishedMapperTasks() {
		return finishedMapperTasks;
	}
	public void setFinishedMapperTasks(int finishedMapperTasks) {
		this.finishedMapperTasks = finishedMapperTasks;
	}
	public int getReducerTaskSplits() {
		return reducerTaskSplits;
	}
	public void setReducerTaskSplits(int reducerTaskSplits) {
		this.reducerTaskSplits = reducerTaskSplits;
	}
	public int getFinishedReducerTasks() {
		return finishedReducerTasks;
	}
	public void setFinishedReducerTasks(int finishedReducerTasks) {
		this.finishedReducerTasks = finishedReducerTasks;
	}
	public ArrayList<SlaveInfo> getReduceLists() {
		return reduceLists;
	}
	public void setReduceLists(ArrayList<SlaveInfo> reduceLists) {
		this.reduceLists = reduceLists;
	}
//	public ArrayList<String> getReducerOutputFile() {
//		return ReducerOutputFile;
//	}
//	public void setReducerOutputFile(ArrayList<String> reducerOutputFile) {
//		ReducerOutputFile = reducerOutputFile;
//	}
	public InetAddress getAddress() {
		return address;
	}
	public void setAddress(InetAddress address) {
		this.address = address;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	
	
}
