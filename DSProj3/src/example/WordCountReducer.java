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
			sum += (Integer) val.getValue();
		}
		context.write(key, new FixValue(sum));
		// context.write((Text) key, new IntWritable(wordCount));
	}
}