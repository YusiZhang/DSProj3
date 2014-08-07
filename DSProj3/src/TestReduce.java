import io.FixValue;
import io.Text;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;

import debug.Printer;
import mapred.Context;
import mapred.Reducer;
import mapred.ReduceTracker.KeyValuePair;
import mapred.Task;


public class TestReduce {
	
	
	ArrayList<String> mapperOutputs = new ArrayList<String>();
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
	
	
	ArrayList<KeyValuePair> pairs = new ArrayList<KeyValuePair>();
	
	//read file 
	public void readFile() throws Exception{
		for (String mapperOutputFile : mapperOutputs) {
			// FileReader fr;
			BufferedReader br = new BufferedReader(new FileReader(mapperOutputFile));
			String line;
			while ((line = br.readLine()) != null) {
//				System.out.println("");
				KeyValuePair kvp = parseLine(line);
				pairs.add(kvp);
				System.out.println("key = "+kvp.key.toString() + ", value = "+kvp.value);
			}
		}
	}
	
	
	private KeyValuePair parseLine(String line) {
		String[] arr = line.split(":");
		Text key = new Text(arr[0]);
		FixValue value = new FixValue(arr[1]);
		KeyValuePair kvp = new KeyValuePair(key, value);
		return kvp;
	}
	
	
	//merge 
	public HashMap<Text, ArrayList<FixValue>> mergeKeyValuePairs(ArrayList<KeyValuePair> pairs) {
		System.out.println("Merge starts");
		HashMap<Text, ArrayList<FixValue>> reduceResult = new HashMap<Text, ArrayList<FixValue>>();
		for (KeyValuePair kvp : pairs) {
			if (reduceResult.containsKey(kvp.key)) {
				reduceResult.get(kvp.key).add(kvp.value);
			} else {
				reduceResult.put(kvp.key, new ArrayList<FixValue>());
				reduceResult.get(kvp.key).add(kvp.value);
			}
			System.out.println("key = " + kvp.key + ", value = "+reduceResult.get(kvp.key));
		}
		System.out.println("Merge ends");
//		/*for test*/
//		Printer.printT(reduceResult);
		return reduceResult;

	}
	
	//start reduce
	private void performReducer(HashMap<Text, ArrayList<FixValue>> reduceResult) {
		// prepare args for reducer
		Class reduceClass;
		Task taskInfo = new Task();
		try {
			reduceClass = Class.forName("example.WordCountReducer");
			Constructor<?> objConstructor = reduceClass.getConstructor();
			Reducer reducer = (Reducer) objConstructor.newInstance();
			
			Context context = new Context(2,"src/");
			
			
			for(Text key : reduceResult.keySet()) {
				reducer.reduce(key, reduceResult.get(key), context);
			}
			context.close();
			System.out.println("perform reduce ends");
			
		} catch (Exception e) {
			System.out.println("internal error from perform reducer");
			e.printStackTrace();
		} 
		


	}
	
	//output file
	
	
	public static void main(String[] args) throws Exception{
		TestReduce t = new TestReduce();
		t.mapperOutputs.add("reduceFile.txt");
		t.readFile();
		
		HashMap<Text, ArrayList<FixValue>> reduceResult = t.mergeKeyValuePairs(t.pairs);
		
		t.performReducer(reduceResult);
		
//		Text t1 = new Text("abc");
//		Text t2 = new Text("abc");
//	System.out.println(t1.equals(t2));
//		String str = "abc:1";
//		String[] arr = str.split("\\:");
//		System.out.println(arr[1]);
	}
}
