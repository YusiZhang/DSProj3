package example;

import mapred.*;


public class WordCountReducer extends Reducer{

@Override
public void reduce(Writable<?> key, Iterable<Writable<?>> values,
        Context context) {
    int wordCount = 0;
    for (Writable<?> value : values) {
        //wordCount += ((IntWritable) value).get();
    }

    //context.write((Text) key, new IntWritable(wordCount));
}
}