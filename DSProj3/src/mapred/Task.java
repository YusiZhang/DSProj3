package mapred;

import java.io.Serializable;

import node.SlaveInfo;
import communication.Message;


public class Task extends Job implements Serializable{
		
	private static final long serialVersionUID = 7767827921605432948L;
	private int taskId;
//	protected String taskName;
	private String TaskClass;
	private SlaveInfo reduceSlave;
	
	
	public Task(String taskName) {
		super(taskName);
		// TODO Auto-generated constructor stub
	}

	public Task() {
		// TODO Auto-generated constructor stub
	}

	public String getTaskClass() {
		return TaskClass;
	}

	public void setTaskClass(String taskClass) {
		TaskClass = taskClass;
	}

	public int getTaskId() {
		return taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
		
	}

	public SlaveInfo getReduceSlave() {
		return reduceSlave;
	}

	public void setReduceSlave(SlaveInfo reduceSlave) {
		this.reduceSlave = reduceSlave;
	}
}
