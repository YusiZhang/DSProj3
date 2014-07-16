package mapred;

public class FixValue {
	private Object value;
	
	public FixValue(){
		
	}
	public FixValue(Object value) {
		this.value = value;
	}
	
	public Object getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public String toString(){
		return this.value.toString();
	}
	
	public int hashCode(){
		return value.hashCode();
	}
	
	public void readData(Object data){
		this.value = data;
	}
}
