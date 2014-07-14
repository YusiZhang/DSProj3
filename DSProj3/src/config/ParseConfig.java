package config;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;

/*
 * 1. parse config file 
 */
public class ParseConfig {
	
	//the port the master listening on
	public static int MasterMainPort;
	
	public static int SlaveMainPort;
	//the ip address of master
	public static int SlaveHeartBeatPort;
	
	public static String MasterIP;
	//master's start port of port pool
	public static int StartPort;
	//master's end port of port pool
	public static int EndPort;
	//file blk chunk size
	public static long ChunkSize;
	//number of file replica
	public static int Replica;
	public static int HearBeatFreq;
	public static String FS_LOC = "dfs/";
	public static String HTTP_PREFIX = "http://";
	
	/*
	 * Parse config file
	 */
	public ParseConfig(String configName) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(configName));
		HashMap<String,String> args = new HashMap<String, String>();
		String line;
		
		while((line = reader.readLine()) != null) {
			System.out.println(line);
			int split = line.indexOf("=");
			if(split != -1) {
				//put key-value pair from config file to args
				args.put(line.substring(0,split).trim(), line.substring(split+1,line.length()-1).trim());
			}
		}
		
		reader.close();
		parseArgs(args);
	}

	private void parseArgs(HashMap<String, String> args) {
		try {
			MasterMainPort = Integer.parseInt(args.get("MasterMainPort"));
			SlaveMainPort = Integer.parseInt(args.get("SlaveMainPort"));
			SlaveHeartBeatPort = Integer.parseInt(args.get("SlaveHeartBeatPort"));
			MasterIP = args.get("MasterIP");
			StartPort = Integer.parseInt(args.get("StartPort"));
			EndPort = Integer.parseInt(args.get("EndPort"));
			ChunkSize = Integer.parseInt(args.get("ChunkSize"));
			HearBeatFreq = Integer.parseInt(args.get("HearBeatFreq"));
			Replica = Integer.parseInt(args.get("Replica"));
			if(MasterIP == null) {
				throw new Exception();
			}
			
		} catch (Exception e) {
			System.out.println("Args are missing or in wrong format" + e.toString());
		}
		
	}
	
}
