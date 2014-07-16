package mapred;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import mapred.Mapper;
import node.SlaveInfo;

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
			
			//send files to reducers
			sendFiles();
			//send success ack to master
			sendSuccess();
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
	}



	private void sendSuccess() {
		
	}



	private void sendFiles() {
		
	}
}
