package example;

import io.FixValue;
import io.Text;
import io.Writable;
import mapred.*;

public static class NGramGenerationReducer extends Reducer {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
         context.write(key, new FixValue(sum)); 
      }
  }