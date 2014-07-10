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
		try {

		} catch (Exception e) {
			System.out.println("Master main port listener is wrong "
					+ e.toString());
			System.exit(0);
		}

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
					Socket soc = null;
					//get message from socket
					Message fileName = Message.receive(soc);
					new FileTransfer.Download(fileName.getContent().toString(),soc);

				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				break;
			case KEEP_ALIVE:
				break;

			default:
				break;
			}

			try {
				socket.close();
				listener.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	private void receiveFile() {
		// TODO Auto-generated method stub

	}
}
