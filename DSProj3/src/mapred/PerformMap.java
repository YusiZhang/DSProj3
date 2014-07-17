package mapred;

import io.LongWritable;
import io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import communication.Message;
import communication.Message.MSG_TYPE;
import dfs.FileTransfer;
import mapred.Mapper;
import node.SlaveInfo;
import node.SlaveMain;

public class PerformMap extends Thread{
	private ArrayList<SlaveInfo> reducersList;
	//baseFileName includes blk id
	private String baseFileName;
	private String mapperClass;
	private Task taskInfo;
	
	public PerformMap(Task taskInfo){
		this.taskInfo = taskInfo;
		this.reducersList= taskInfo.getReduceLists();
		this.baseFileName = taskInfo.getInputFileName();
		this.mapperClass = taskInfo.getMapperClass();
		
		System.out.println("get mapper class name!!!"+taskInfo.getMapperClass());
		System.out.println("mapper class name !!! "+this.mapperClass);
	}
	
	
	
	@Override
	public void run() {
		super.run();
		try {
			
			//prepare and perform map function
			performMapper();
			//send files to reducers
			
			/*for test!!!!!*/
			Random random = new Random();
			int next = random.nextInt();
			Thread.sleep(next * 2000);
			/*for test only!!!!*/
			
			
			sendFiles();
			//send success ack to master
			sendSuccess();
			
		} catch (Exception e) {
			System.out.println("internal error from run perform map");
			e.printStackTrace();
		} 
		
	}


	/*
	 *basefilename is like file_blk0.txt 
	 *filename : <jobid>_<taskid>_MapResult
	 */
	private void performMapper() throws Exception {
		//prepare args for mapper
		Class mapClass = Class.forName("example."+mapperClass);
		Constructor<?> objConstructor = mapClass.getConstructor();
		Mapper mapper = (Mapper) objConstructor.newInstance();
		
		//filename is to be written later
		String filename = this.taskInfo.getJobId()+"_"+this.taskInfo.getTaskId()+"_MapResult";
		Context context = new Context(this.reducersList.size(), filename);
		//process line by line
		FileReader fd = new FileReader(this.baseFileName);
        BufferedReader reader = new BufferedReader(fd);
        String line;
        long lineNum = 0;
		while((line = reader.readLine()) != null){
			mapper.map(new LongWritable(lineNum++), new Text(line), context);
		}
		
		reader.close();
		fd.close();
		
	}


	/*
	 * send task info to master
	 * mapper_done 
	 */
	private void sendSuccess() throws Exception {
		Message successMsg = new Message(MSG_TYPE.MAPPER_DONE,this.taskInfo);
		Socket soc = new Socket(SlaveMain.conf.MasterIP,SlaveMain.conf.MasterMainPort);
		successMsg.send(soc);
	}


	/*
	 * upload filename : <jobid>_<taskid>_MapResult
	 * for download filename : <jobid>_<taskid>_Reduce
	 */
	private void sendFiles() throws Exception {
		for(int i = 0; i < reducersList.size() ; i++) {
			Socket soc = new Socket(reducersList.get(i).address,SlaveMain.conf.SlaveMainPort);
			String downloadFile = this.taskInfo.getJobId()+"_"+this.taskInfo.getTaskId()+"_Reduce";
			Message msg = new Message(MSG_TYPE.MAPRESULT_TO_REDUCE,downloadFile);
			msg.send(soc);
			new FileTransfer.Upload(this.taskInfo.getJobId()+"_"+this.taskInfo.getTaskId()+"_MapResult"+i, soc).start();
			Thread.sleep(500);
		}
	}
}
