package node;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import config.ParseConfig;

/*
 * For dfs, master should do following things
 * handled by (master) scheduler:
 * 1. receive files info from client, update slave table
 * and reply layout info to client.
 * 2. find files
 * 3. heartbeat slave
 * 4. upload file
 */
public class MasterMain {
	
	//table for slave info : socketAddr-slaveInfo
	public static ConcurrentHashMap<SocketAddress,SlaveInfo> slavePool = new ConcurrentHashMap<SocketAddress, SlaveInfo>();
	
	//currently used port
	public static int curPort;
	
	public static void main(String[] args) {
		try {
			new ParseConfig(args[0]);
			curPort = ParseConfig.StartPort;
		} catch (Exception e) {
			System.out.println("Something is wrong with config file " + e.toString());
			System.exit(1);
		}
		
		//run scheduler, listen on MasterMainPort
		Scheduler scheduler = new Scheduler();
		scheduler.start();
		
		
	}
}
