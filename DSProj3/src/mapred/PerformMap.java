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
	
	public PerformMap(Task taskInfo){
		this.reducersList= taskInfo.getReduceLists();
		this.baseFileName = taskInfo.getInputFileName();
		this.mapperClass = taskInfo.getMapperClass();
	}
	
	
	
	@Override
	public void run() {
		super.run();
		try {
			
			//prepare and perform map function
			performMapper();
			//send files to reducers
			sendFiles();
			//send success ack to master
			sendSuccess();
			
		} catch (Exception e) {
			System.out.println("internal error from run perform map");
			e.printStackTrace();
		} 
		
	}


	/*
	 *questions: what base file name is? 
	 */
	private void performMapper() throws Exception {
		//prepare args for mapper
		Class mapClass = Class.forName("mapred."+mapperClass);
		Constructor<?> objConstructor = mapClass.getConstructor();
		Mapper mapper = (Mapper) objConstructor.newInstance();
		
		Context context = new Context(this.reducersList.size(), this.baseFileName);
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
	 * questions: what info send to master?
	 * mapper_done or mapper_success?
	 */
	private void sendSuccess() throws Exception {
		Object content = new Object();
		Message successMsg = new Message(MSG_TYPE.MAPPER_DONE, (Serializable) content);
		Socket soc = new Socket(SlaveMain.conf.MasterIP,SlaveMain.conf.MasterMainPort);
		successMsg.send(soc);
	}


	/*
	 * questions: except files, what others should be sent?
	 * i.e. job id? (it may be included in baseFileName)
	 */
	private void sendFiles() throws Exception {
		for(int i = 0; i < reducersList.size() ; i++) {
			Socket soc = new Socket(reducersList.get(i).address,SlaveMain.conf.SlaveMainPort);
			new FileTransfer.Upload(baseFileName+i, soc);
		}
	}
}
