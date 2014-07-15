package mapred;

public class JobConf {
	private Class<?> inputFormat;
	private Class<?> outputFormat;
	private Class<?> mapperClass;
	private Class<?> reducerClass;
	private Class<?> inputFile;
	private Class<?> outputFile;
	
	public Class<?> getMapperClass() {
		return mapperClass;
	}
	public void setMapperClass(Class<?> mapperClass) {
		this.mapperClass = mapperClass;
	}
	public Class<?> getReducerClass() {
		return reducerClass;
	}
	public void setReducerClass(Class<?> reducerClass) {
		this.reducerClass = reducerClass;
	}
	public Class<?> getInputFile() {
		return inputFile;
	}
	public void setInputFile(Class<?> inputFile) {
		this.inputFile = inputFile;
	}
	public Class<?> getOutputFile() {
		return outputFile;
	}
	public void setOutputFile(Class<?> outputFile) {
		this.outputFile = outputFile;
	}
	public Class<?> getInputFormat() {
		return inputFormat;
	}
	public void setInputFormat(Class<?> inputFormat) {
		this.inputFormat = inputFormat;
	}
	public Class<?> getOutputFormat() {
		return outputFormat;
	}
	public void setOutputFormat(Class<?> outputFormat) {
		this.outputFormat = outputFormat;
	}
	
	
}
