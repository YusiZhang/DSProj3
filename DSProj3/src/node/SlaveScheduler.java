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
import dfs.FileTransfer;

public class SlaveScheduler extends Thread {
	// public static int curPort = ParseConfig.StartPort;
	public static int slaveId = 0;
	ServerSocket listener;
	Socket socket;
	Message msg;
	ParseConfig conf;

	public SlaveScheduler(int port) throws IOException {
		listener = new ServerSocket(port);
		socket = null;
		msg = null;
		conf = SlaveMain.conf;
	}

	public void run() {

		while (true) {

			try {
				socket = listener.accept();
				msg = Message.receive(socket);
				System.out.println("the scheduler receives a " + msg.getType()
						+ " messge");

			} catch (Exception e) {
				System.out.println("There is no message " + e.toString());
				continue;
			}

			switch (msg.getType()) {

			case FILE_PUT_REQ_TO_SLAVE:
				fileDownloadHandler( msg,  socket);
				break;
				
			case KEEP_ALIVE:
				Message reply = new Message(Message.MSG_TYPE.KEEP_ALIVE, null);
				try {
					reply.send(socket);
					System.out.println("respond to master heart beat");
					
				} catch (Exception e) {
					System.out.println("fail to respond to heart beat");
					e.printStackTrace();
				}
				break;
			case NEW_MAP_TO_SLAVE:
				newMapHandler(msg,socket);
				break;
			case NEW_REDUCE_TO_SLAVE:
				newReduceHandler(msg,socket);
				break;
			default:
				break;
			}
			


		}
	}
	
	/*
	 * Handle reduce task
	 */
	private void newReduceHandler(Message msg2, Socket socket2) {
		
	}
	
	/*
	 * Handle map task
	 * In msg, there should be reduce slave info. Maper slave can know where to send result.
	 */
	private void newMapHandler(Message msg, Socket socket) {
			
	}

	private void fileDownloadHandler(Message msg, Socket socket) {
		System.out.println("Message received from "
				+ socket.getRemoteSocketAddress()
				+ " type: FILE_PUT_REQ_TO_SLAVE; content: "
				+ msg.getContent().toString());
		SlaveMain.curPort++;

		Message reply = new Message(Message.MSG_TYPE.PORT,
				SlaveMain.curPort);

		// tell the client which slaves are available
		try {
			reply.send(socket);
			System.out.println("send the reply to client "
					+ reply.getContent());
			System.out.println("send the port number from the slave to the client");

			//create listener , wait for reply from client
			ServerSocket slaveListener = new ServerSocket(SlaveMain.curPort);
			//new socket from client
			Socket fileDownloadSoc = slaveListener.accept();
			System.out.println("new socket from client" + fileDownloadSoc.getRemoteSocketAddress());
			//get message from socket
			Message fileName = Message.receive(fileDownloadSoc);
			if(fileName != null){
				new FileTransfer.Download(fileName.getContent().toString(),fileDownloadSoc,conf.ChunkSize).start();
			}
			

		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
	}


}
