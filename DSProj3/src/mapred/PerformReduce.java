package mapred;

//
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
//import java.net.Socket;
//import java.net.UnknownHostException;
//import java.io.FileReader;
import java.util.*;

//import java.util.HashMap;
//
import communication.Message;
import config.ParseConfig;
import debug.Printer;
import io.FixValue;
import io.Text;
import io.Writable;

//
public class PerformReduce extends Thread {
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

	public PerformReduce(Task taskInfo){
		this.taskInfo = taskInfo;
		File folder = new File("src/");
		File[] listOfFiles = folder.listFiles();
		for (File file : listOfFiles) {
		    if (file.isFile()) {
		    	System.out.println("file in src name is:" +file.getName());
		    	String[] parts = file.getName().split("_");
		    	if(parts[0].equals(taskInfo.getJobId())&&parts[2].equals("Reduce")){
		    		//add file name into mapper outputs
		    		mapperOutputs.add(file.getName());
		    		System.out.println("Right file!");
		    	}else {
		    		System.out.println("Wrong file!");
		    	}
		    }
		}
	}
	
	public void run() {

		try {
			// read all the mappers output file into one file
			for (String mapperOutputFile : mapperOutputs) {
				// FileReader fr;
				BufferedReader br = new BufferedReader(new FileReader(mapperOutputFile));
				String line;
				while ((line = br.readLine()) != null) {
					KeyValuePair kvp = parseLine(line);
					pairs.add(kvp);
				}
			}

			// sort the pairs
			sortKeyValuePairs(pairs);

			// merge the pairs
			HashMap<Text, ArrayList<FixValue>> reduceResult = mergeKeyValuePairs(pairs);

			// performa reducer()
			performReducer(reduceResult);

			// send the result back to master

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void performReducer(HashMap<Text, ArrayList<FixValue>> reduceResult) {
		// prepare args for reducer
		Class reduceClass;
		try {
			reduceClass = Class.forName("mapred." + taskInfo.getReducerClass());
			Constructor<?> objConstructor = reduceClass.getConstructor();
			Reducer reducer = (Reducer) objConstructor.newInstance();
			Context context = new Context(1,((Integer)taskInfo.jobId).toString());

			
		} catch (Exception e) {
			System.out.println("internal error from perform reducer");
			e.printStackTrace();
		} 
		


	}

	private HashMap<Text, ArrayList<FixValue>> mergeKeyValuePairs(ArrayList<KeyValuePair> pairs) {
		System.out.println("Merge starts");
		HashMap<Text, ArrayList<FixValue>> reduceResult = new HashMap<Text, ArrayList<FixValue>>();
		for (KeyValuePair kvp : pairs) {
			if (reduceResult.containsKey(kvp.key)) {
				reduceResult.get(kvp.key).add(kvp.value);
			} else {

				reduceResult.put(kvp.key, new ArrayList<FixValue>());
				reduceResult.get(kvp.key).add(kvp.value);
			}
		}
		System.out.println("Merge ends");
		/*for test*/
		Printer.printT(reduceResult);
		return reduceResult;

	}

	private void sortKeyValuePairs(ArrayList<KeyValuePair> pairs) {
		System.out.println("sort starts");
		Collections.sort(pairs);
		System.out.println("sort ends");
	}

	private KeyValuePair parseLine(String line) {
		String[] arr = line.split(":");
		Text key = new Text(arr[0]);
		FixValue value = new FixValue(arr[1]);
		KeyValuePair kvp = new KeyValuePair(key, value);
		return kvp;
	}

}
//
//
// HashMap<Writable<?>, Writable<?>> results = new HashMap<Writable<?>,
// Writable<?>>();
// public PerformReduce() {
// // System.out.println();
//
// }
//
//
// public void run(){
// for (int i = 0; i < mapperOutputs.size(); i++) {
// try {
// FileReader fr = new FileReader(mapperOutputs.get(i));
// BufferedReader br = new BufferedReader(fr);
// ArrayList<KeyValuePair> pairs = new ArrayList<KeyValuePair>();
// String line;
//
// while (true) {
// line = br.readLine();
// if (line == null) break;
// else {
// pairs.add(parseRecord(line, i));
//
// //add the key value pair into the final result
//
// }
// }
// br.close();
// fr.close();
//
//
// new File(mapperOutputs.get(i)).delete();
//
// //write the pairs to a file
//
// } catch (FileNotFoundException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// } catch (IOException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
//
// }
//
// //merge all the result files
// String reducerResult = null;
// reducerResult = merge(outputs);
//
// //send the reducer result msg to master
// sendResult();
// }
//
// private void sendResult() {
// try {
// Socket socket = new Socket(ParseConfig.MasterIP, ParseConfig.MasterMainPort);
// //what is the content of the msg???
// new Message(Message.MSG_TYPE.REDUCER_DONE, null).send(socket);;
//
// Message.receive(socket);
// socket.close();
//
// } catch (UnknownHostException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// } catch (IOException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// } catch (Exception e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
//
//
// }
//
// private String merge(ArrayList<String> outputs) {
// return null;
// }
//
// private KeyValuePair parseRecord(String line, int i) {
// // TODO Auto-generated method stub
// return null;
// }
//
// }
