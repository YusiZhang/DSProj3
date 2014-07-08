package node;

import java.net.ServerSocket;
import java.net.Socket;

import communication.Message;
import config.ParseConfig;

public class Scheduler extends Thread{
	public void run() {
		ServerSocket listener = null;
		try {
			listener = new ServerSocket (ParseConfig.MasterMainPort);
		} catch (Exception e) {
			System.out.println("Master main port listener is wrong " + e.toString());
			System.exit(0);
		}
		
		while(true) {
			Socket socket = null;
			Message msg = null;
			try {
				socket = listener.accept();
				msg = Message.receive(socket);
			} catch (Exception e) {
				System.out.println("There is no message " + e.toString());
				continue;
			}
			
			switch (msg.getType()) {
			case FILE_PUT_REQ:
				
				Message reply = new Message(Message.MSG_TYPE.FILE_PUT_ACK, "Message Received plz use new soc for uploading");
				System.out.println("Message received from " + socket.getRemoteSocketAddress() + "type: FILE_PUT_REQ; content: " + msg.getContent().toString());
				
				try {
					reply.send(socket);
					
					socket.close();
				} catch (Exception e) {
					System.out.println("Reply message is wrong " + e.toString());
				}
				break;

			default:
				break;
			}
		}
	}
}
