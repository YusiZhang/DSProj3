package node;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class SlaveInfo implements Serializable{
	//how to keep slave id global?
	public int slaveId;
	public InetAddress address;
	public SlaveInfo(int id, InetAddress add){
		slaveId = id;
		address = add;
	}
	
}
