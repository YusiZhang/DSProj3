package mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;

public class Context implements Serializable{
	private int reduceSize;
	private String baseName;
	private ArrayList<PrintWriter> writerList;
	
	public Context () {
		
	}
	
	public Context(int reduceSize, String  baseName) {
		this.reduceSize = reduceSize;
		this.baseName = baseName;
		initPrintWriter();
	}

	private void initPrintWriter() {
		for(int i = 0; i < reduceSize; i++) {
			try {
				PrintWriter writer = new PrintWriter(new File(baseName + i));
				writerList.add(writer);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
	
	//contraints
	public void write(Text key, FixValue value) {
		PrintWriter writer = writerList.get(key.hashCode() % reduceSize);
		writer.println(key.getValue() + ":" + value.getValue());
	}
	
	public void close(){
		for(PrintWriter writer : writerList){
			writer.close();
		}
	}
	
	
	
	
}
