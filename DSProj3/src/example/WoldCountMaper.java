package example;

import io.FixValue;
import io.LongWritable;
import io.Text;
import mapred.Context;
import mapred.Mapper;

public class WoldCountMaper extends Mapper{

	@Override
	public void map(LongWritable key, Text value, Context context) {
		FixValue fixValue = new FixValue(1); 
		String curLine = value.getValue();	
		String[] wordArray = curLine.split("\\s+");
		for(String word : wordArray) {
			context.write(new Text(word), fixValue);
		}
		
	}

}
