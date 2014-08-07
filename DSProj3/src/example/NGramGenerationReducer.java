package example;

import io.FixValue;
import io.Text;
import io.Writable;
import mapred.*;

public class NGramGenerationReducer extends Reducer {

	@Override
    public void reduce(Text key, Iterable<FixValue> values, Context context)   {
      int sum = 0;
      for (FixValue value : values) {
        sum += Integer.parseInt(value.getValue().toString());
      }
         context.write(key, new FixValue(sum)); 
         
      }
  }