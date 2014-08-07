package mapred;

//
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.UnknownHostException;
//import java.net.Socket;
//import java.net.UnknownHostException;
//import java.io.FileReader;
import java.util.*;

import node.FileInfo;
import node.SlaveMain;
//import java.util.HashMap;
//
import communication.Message;
import communication.Message.MSG_TYPE;
import communication.ReducerDoneMsg;
import config.ParseConfig;
import debug.Printer;
import dfs.FileTransfer;
import io.FixValue;
import io.Text;
import io.Writable;

//
public class ReduceTracker extends Thread {
	ArrayList<String> mapperOutputs;
	ArrayList<String> outputs;
	ArrayList<KeyValuePair> pairs = new ArrayList<KeyValuePair>();
	Task taskInfo;

	public class KeyValuePair implements Comparable<KeyValuePair> {

		public Text key;
		public FixValue value;

		public KeyValuePair(Text k, FixValue v) {
			this.key = k;
			this.value = v;
		}

		@Override
		public int compareTo(KeyValuePair kv1) {
			return this.key.hashCode() - kv1.key.hashCode();
		}
	}

	public ReduceTracker(Task taskInfo) {
		this.taskInfo = taskInfo;
		this.mapperOutputs = new ArrayList<String>();
		File folder = new File("./");
		File[] listOfFiles = folder.listFiles();
		System.out.println("list of files" + listOfFiles.length);
		for (File file : listOfFiles) {
			String[] parts = file.getName().split("_");
			if (parts[0].equals(taskInfo.getJobId() + "")
					&& parts[2].equals("Reduce")) {
				System.out.println("perform reduce with the right file");

				mapperOutputs.add(file.getName());
			}
		}
		// }
	}

	public void run() {

		try {
			System.out.println("Starting to read reduce file");
			// read all the mappers output file into one file
			for (String mapperOutputFile : mapperOutputs) {
				// FileReader fr;
				BufferedReader br = new BufferedReader(new FileReader(
						mapperOutputFile));
				String line;
				int count = 0;
				while ((line = br.readLine()) != null) {

					KeyValuePair kvp = parseLine(line);
					if (kvp != null)
						pairs.add(kvp);
				}
			}

			// sort the pairs
			sortKeyValuePairs(pairs);

			// merge the pairs
			HashMap<Text, ArrayList<FixValue>> reduceResult = mergeKeyValuePairs(pairs);

			// performa reducer()
			performReducer(reduceResult);

			System.out.println("here the reduceResult size: "
					+ reduceResult.size());

			sendResultFileNameToMaster();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sendResultFileNameToMaster() throws IOException, Exception {
		String fileName = taskInfo.getJobId() + "_" + taskInfo.getTaskId()
				+ "_reduceResult0";
		FileInfo fileInfo = new FileInfo();
		fileInfo.fileName = fileName;
		fileInfo.slaveInfo = taskInfo.getReduceSlave();
		fileInfo.taskInfo = taskInfo;
		Message msg = new Message(MSG_TYPE.REDUCER_DONE, fileInfo);
		Socket socket = new Socket(SlaveMain.conf.MasterIP,
				SlaveMain.conf.MasterMainPort);
		msg.send(socket);
	}

	private void sendResultToMaster(String uploadFile) throws Exception {
		Socket soc = new Socket(SlaveMain.conf.MasterIP,
				SlaveMain.conf.MasterMainPort);

		String fileName = taskInfo.getJobId() + "_" + taskInfo.getTaskId()
				+ "_reduceResult0";

		ReducerDoneMsg replyContent = new ReducerDoneMsg(taskInfo, null);
		replyContent.fileName = fileName;

		Message msg = new Message(MSG_TYPE.REDUCER_DONE, replyContent);
		msg.send(soc);
		/* for test!!!!! */
		Random random = new Random();
		int next = random.nextInt(3);
		Thread.sleep(next * 2000);
		/* for test only!!!! */

		new FileTransfer.Upload(fileName.substring(0, fileName.length()), soc)
				.start();

	}

	private void performReducer(HashMap<Text, ArrayList<FixValue>> reduceResult) {
		// prepare args for reducer
		Class reduceClass;
		try {
			reduceClass = Class
					.forName("example." + taskInfo.getReducerClass());
			Constructor<?> objConstructor = reduceClass.getConstructor();
			Reducer reducer = (Reducer) objConstructor.newInstance();

			Context context = new Context(1,
					((Integer) taskInfo.jobId).toString() + "_"
							+ taskInfo.getTaskId() + "_reduceResult");

			for (Text key : reduceResult.keySet()) {
				reducer.reduce(key, reduceResult.get(key), context);
			}
			// CLOSE!!!!
			context.close();
			System.out.println("perform reduce ends");

		} catch (Exception e) {
			System.out.println("internal error from perform reducer");
			e.printStackTrace();
		}

	}

	private HashMap<Text, ArrayList<FixValue>> mergeKeyValuePairs(
			ArrayList<KeyValuePair> pairs) {
		System.out.println("Merge the key value pairs");
		HashMap<Text, ArrayList<FixValue>> reduceResult = new HashMap<Text, ArrayList<FixValue>>();
		for (KeyValuePair kvp : pairs) {
			if (reduceResult.containsKey(kvp.key)) {
				reduceResult.get(kvp.key).add(kvp.value);
			} else {

				reduceResult.put(kvp.key, new ArrayList<FixValue>());
				reduceResult.get(kvp.key).add(kvp.value);
			}
		}
		/* for test */
		// Printer.printT(reduceResult);
		return reduceResult;

	}

	private void sortKeyValuePairs(ArrayList<KeyValuePair> pairs) {
		System.out.println("sort the key value pairs");
		Collections.sort(pairs);
	}

	private KeyValuePair parseLine(String line) {

		String[] arr = line.split("\\:");
		Text key = new Text(arr[0]);
		if (arr.length <= 1)
			return null;
		FixValue value = new FixValue(arr[1]);
		KeyValuePair kvp = new KeyValuePair(key, value);
		return kvp;

	}

}