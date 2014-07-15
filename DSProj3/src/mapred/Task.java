package mapred;

import java.io.Serializable;


public class Task extends Job implements Serializable{
	
	public Task(String jobName) {
		super(jobName);
	}

	public String fileName;
}
