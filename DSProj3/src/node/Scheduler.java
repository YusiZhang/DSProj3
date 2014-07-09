package node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import communication.Message;
import config.ParseConfig;

public class Scheduler extends Thread{
	public static ConcurrentHashMap<Integer,SlaveInfo> slavePool = new ConcurrentHashMap<Integer,SlaveInfo>();
	public int port;
	public static int slaveId = 0;
	ServerSocket listener;
	public Scheduler(int port) throws IOException{
		listener = new ServerSocket(port);
	}
	
	public void run() {
//		try {
//		
//		} catch (Exception e) {
//			System.out.println("Master main port listener is wrong " + e.toString());
//			System.exit(0);
//		}
		
		while(true) {
			Socket socket = null;
			Message msg = null;
			try {
				socket = listener.accept();
				
				//add the new slave into the registry list
//				InetAddress address = socket.getInetAddress();				
				
				msg = Message.receive(socket);
				System.out.println("the master receives a "+msg.getType()+" messge");
				
			} catch (Exception e) {
				System.out.println("There is no message " + e.toString());
				continue;
			}
			
			switch (msg.getType()) {
			case REG_NEW_SLAVE:
				SocketAddress address = socket.getRemoteSocketAddress();
				SlaveInfo slave = new SlaveInfo(slaveId, address);
				slaveId++;
				System.out.println("connect to slave "+slave.slaveId+ " "+ address);
				slavePool.put(slaveId,slave);
				break;
			case FILE_PUT_REQ:
//				Message reply = new Message(Message.MSG_TYPE.FILE_PUT_ACK, "Message Received. Please use a new socket for uploading");
				ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>();
				for (int s: slavePool.keySet()){
					slaveList.add(slavePool.get(s));
				}
				System.out.println("Message received from " + socket.getRemoteSocketAddress() + " type: FILE_PUT_REQ; content: " + msg.getContent().toString());
				Message reply = new Message(Message.MSG_TYPE.AVAIL_SLAVES, slaveList);
			
				//tell the client which slaves are available
				try {
					
					reply.send(socket);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				System.out.println("send the slave list from the master to the client");
				System.out.println(slaveList.toString());
//				try {
//					
//					
//					socket.close();
//				} catch (Exception e) {
//					System.out.println("Reply message is wrong " + e.toString());
//				}
				break;

			default:
				break;
			}
			/*
			try {
				socket.close();
				listener.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
		}
	}
}
