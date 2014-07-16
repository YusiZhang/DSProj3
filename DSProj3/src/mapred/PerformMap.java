package mapred;

import java.util.ArrayList;

import node.SlaveInfo;

public class PerformMap extends Thread{
	private ArrayList<SlaveInfo> reducersList;
	private String baseFileName;
	private String mapperClass;
	
	public PerformMap(Task taskInfo){
	}
	
	
	
	@Override
	public void run() {
		super.run();
		
	}
}
