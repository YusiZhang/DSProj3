package mapred;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
public class JobClient {
	/*http://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/mapred/JobClient.html
	*/
	
	private static JobConf jobConf;
	private static Registry registry;
	private static String jobHostName;
	private static int jobPort;
	private static String JobServiceName;
	
	public void runJob(JobConf conf){
		JobClient jc = new JobClient();
		
		
		//submit the job
		jc.submitJob(conf);
		
		//return after the job is completed
		
	}
	
	public static JobConf getJobConf() {
		return jobConf;
	}

	public static void setJobConf(JobConf jobConf) {
		JobClient.jobConf = jobConf;
		
	}

	public void submitJob(JobConf job){
		//submitJobInternal(job);
		JobTracker jobTracker = null;
		
	}

	
	

}
