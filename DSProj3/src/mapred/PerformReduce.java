package mapred;

public class PerformReduce extends Thread {
	
	public class KeyValuePair implements Comparable<KeyValuePair>{
		
		public Writable<?> key;
		public Writable<?> value;
		
		@Override
		public int compareTo(KeyValuePair kv1) {
			return kv1.key.hashCode() - this.key.hashCode();
		}
	}
	
	public void run(){
		 
	}
	
}
