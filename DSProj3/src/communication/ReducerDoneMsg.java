package communication;

import io.*;

import java.io.Serializable;
import java.util.*;

import mapred.Task;

public class ReducerDoneMsg implements Serializable {
	/**
	 * the ReducerDoneMsg is sent from a reducer node to master when the reducer task is done
	 */
	private static final long serialVersionUID = 8212358974405295131L;
	public Task task;
	public HashMap<Text, FixValue> ReduceResultMap;
	
	public ReducerDoneMsg(Task task, HashMap<Text, FixValue> map){
		this.task = task;
		this.ReduceResultMap = map;
	}
	
	public String fileName;
}
