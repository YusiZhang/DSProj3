package node;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class SlaveInfo implements Serializable{
	//how to keep slave id global?
	public int slaveId;
	public SocketAddress address;
	public SlaveInfo(int id, SocketAddress add){
		slaveId = id;
		address = add;
	}
}
