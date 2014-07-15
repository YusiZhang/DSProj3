package Example;
import 
import src.Exception;
import src.String;
import src.WordCount.Map;
import src.WordCount.Reduce;

public class WordCountExample {
	public static void main(String[] args) throws Exception {
	    JobConf = new JobConf();
	        
	        Job job = new Job(conf, "wordcount");
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
