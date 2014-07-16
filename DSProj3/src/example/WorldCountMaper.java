package example;

import mapred.Context;
import mapred.FixValue;
import mapred.LongWritable;
import mapred.Mapper;
import mapred.Text;

public class WorldCountMaper extends Mapper{

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
