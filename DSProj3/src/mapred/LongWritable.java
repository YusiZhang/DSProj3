package mapred;

import java.io.Serializable;

public class LongWritable implements Serializable {
	private Long value;


	public LongWritable() {
	}
	
	public LongWritable(Long value){
		this.value = value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public long getValue() {
		return value;
	}

	public String toString() {
		return value.toString();
	}
	
	public int hashCode(){
		return value.hashCode();
	}
	
	public void readData(String data){
		this.value = Long.parseLong(data.trim());
	}

}
