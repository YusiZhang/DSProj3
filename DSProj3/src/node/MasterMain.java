package node;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

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
	
	//
	public static void main(String[] args) {
		
	}
}
