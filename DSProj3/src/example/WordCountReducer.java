package example;

import io.FixValue;
import io.Text;
import io.Writable;
import mapred.*;

public class WordCountReducer extends Reducer {

	@Override
	public void reduce(Text key, Iterable<FixValue> values, Context context) {
		int wordCount = 0;
		int sum = 0;
		for (FixValue val : values) {
			sum += Integer.parseInt(val.getValue().toString());
		}
		context.write(new Text(key.getValue()), new FixValue(sum));
		
		
		try {
			Thread.sleep(50);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}