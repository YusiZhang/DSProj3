package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import config.ParseConfig;

/*
 * ==NameNode===
 * For dfs, master should do following things
 * handled by (master) scheduler:
 * 1. receive files info from client, update slave table
 * and reply layout info to client.
 * 2. find files
 * 3. heartbeat slave
 * 
 */
public class MasterMain {
	
	//table for slave info : socketAddr-slaveInfo
	//public static ConcurrentHashMap<Integer,SlaveInfo> slavePool = new ConcurrentHashMap<Integer, SlaveInfo>();
	
	//public static ConcurrentHashMap<Integer,SlaveInfo> failPool = new ConcurrentHashMap<Integer, SlaveInfo>();
	//currently used port
	public static int curPort;
	public static ParseConfig conf;
	public static void main(String[] args) throws IOException {
		try {
			System.out.println("start master");
			conf = new ParseConfig(args[0]);
			curPort = conf.StartPort;
 
		} catch (Exception e) {
			System.out.println("Something is wrong with config file " + e.toString());
			System.exit(1);
		}
		
		//run scheduler, listen on MasterMainPort
		Scheduler scheduler = new Scheduler(conf.MasterMainPort);
		System.out.println("start the scheduler on "+conf.MasterMainPort);
		scheduler.start();
		
//		MasterHeartBeat heartbeat = new MasterHeartBeat();
//		heartbeat.start();
	}
	
}
