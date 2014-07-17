package io;

import java.io.Serializable;

public class Text  implements Serializable{
	private String value;
	
	public Text(){
		
	}
	public Text(String value) {
		this.value = value;
	}

	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public String toString(){
		return this.value;
	}
	@Override
	public int hashCode(){
		return value.hashCode();
	}
	@Override
	public boolean equals(Object t1){
		return (this.hashCode() == t1.hashCode());
	}
	
	public void readData(String data){
		this.value = data;
	}
	
}
