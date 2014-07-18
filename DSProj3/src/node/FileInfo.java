package node;

import java.io.Serializable;

import mapred.Task;

public class FileInfo implements Serializable{
	public SlaveInfo slaveInfo;
	public String fileName;
	public Task taskInfo;
	public FileInfo(){}
	public FileInfo(SlaveInfo slaveInfo, String fileName) {
		this.slaveInfo = slaveInfo;
		this.fileName = fileName;
	}
}
