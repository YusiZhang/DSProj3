package mapred;

import io.LongWritable;
import io.Text;

public abstract class Mapper {
	public abstract void map(LongWritable key, Text value, Context context);
}
