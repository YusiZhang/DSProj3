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
//	public static int curPort = ParseConfig.StartPort;
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
			ParseConfig conf = null;
			try {
				socket = listener.accept();
				conf = MasterMain.conf;
				//add the new slave into the registry list
//				InetAddress address = socket.getInetAddress();				
				
				msg = Message.receive(socket);
				System.out.println("the scheduler receives a "+msg.getType()+" messge");
				
			} catch (Exception e) {
				System.out.println("There is no message " + e.toString());
				continue;
			}
			
			switch (msg.getType()) {
			case REG_NEW_SLAVE:
				InetAddress address = socket.getInetAddress();
				SlaveInfo slave = new SlaveInfo(slaveId, address);
				slaveId++;
				System.out.println("connect to slave "+slave.slaveId+ " "+ address);
				slavePool.put(slaveId,slave);
				break;
				
			case FILE_PUT_REQ_TO_MASTER:
//				Message reply = new Message(Message.MSG_TYPE.FILE_PUT_ACK, "Message Received. Please use a new socket for uploading");
				ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>();
				for (int s: slavePool.keySet()){
					slaveList.add(slavePool.get(s));
				}
				System.out.println("Message received from " + socket.getRemoteSocketAddress() + " type: FILE_PUT_REQ_TO_MASTER; content: " + msg.getContent().toString());
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
				
			case FILE_PUT_REQ_TO_SLAVE:
				System.out.println("Message received from " + socket.getRemoteSocketAddress() + " type: FILE_PUT_REQ_TO_SLAVE; content: " + msg.getContent().toString());
				SlaveMain.curPort++;

				reply = new Message(Message.MSG_TYPE.PORT, SlaveMain.curPort);
			
				//tell the client which slaves are available
				try {
					reply.send(socket);
					System.out.println("send the reply to client "+reply.getContent());
					System.out.println("send the port number from the slave to the client");

//					Scheduler s1 = new Scheduler(SlaveMain.curPort);
//					System.out.println("listen on the current port "+SlaveMain.curPort);
//					s1.start();
					ServerSocket slaveListener = new ServerSocket(SlaveMain.curPort);
					Socket soc = null;
					soc = slaveListener.accept();
					Message fileMsg = null;
					fileMsg = Message.receive(soc);
					System.out.println("File received from " + fileMsg.getType() +fileMsg.getContent().toString());
					
					
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
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

	private void receiveFile() {
		// TODO Auto-generated method stub
//		System.out.println();
	}
}
