package node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import communication.Message;
import config.ParseConfig;

public class SlaveHeartBeat extends Thread {
	private ServerSocket listener;
	private Socket socket;
	private Message msg;
	public SlaveHeartBeat(){
		ParseConfig conf = SlaveMain.conf;
		try {
			listener = new ServerSocket(conf.SlaveHeartBeatPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
		socket = null;
		msg = null;
	}
	
	@Override
	public void run() {
		System.out.println("start the heart beat thread on slave");
		
		while(true){
			try {
				socket = listener.accept();
				msg = Message.receive(socket);
				
			} catch (Exception e) {
				System.out.println("There is no message " + e.toString());
				continue;
			}
			
			switch (msg.getType()) {
			case KEEP_ALIVE:
				Message reply = new Message(Message.MSG_TYPE.KEEP_ALIVE, "Slave is alive");
				try {
					reply.send(socket);
					System.out.println("respond to master heart beat");
					
				} catch (Exception e) {
					System.out.println("fail to respond to heart beat");
					e.printStackTrace();
				}
				break;

			default:
				break;
			}
		}
		
	}
}
