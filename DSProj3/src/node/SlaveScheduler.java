package node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import mapred.Job;
import mapred.MapTracker;
import mapred.ReduceTracker;
import mapred.Task;
import communication.Message;
import config.ParseConfig;
import dfs.FileTransfer;

public class SlaveScheduler extends Thread {
	// public static int curPort = ParseConfig.StartPort;
	public static int slaveId = 0;
	public static ConcurrentHashMap <Integer,ArrayList<Thread>> jobToThread = new ConcurrentHashMap<Integer, ArrayList<Thread>>(); 
	
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
				System.out.println("Receives a " + msg.getType()
						+ " messge");

			} catch (Exception e) {
				System.out.println("There is no message " + e.toString());
				continue;
			}

			switch (msg.getType()) {

			case FILE_PUT_REQ_TO_SLAVE:
				fileDownloadHandler( msg,  socket);
				break;
				
			
			case NEW_MAPPER:
				newMapHandler(msg,socket);
				break;
			case NEW_REDUCER:
				newReduceHandler(msg,socket);
				break;
			case MAPRESULT_TO_REDUCE:
				downloadReduceResult(msg,socket);
				break;
			case FILEDOWNLOAD:
				fileGetContentRequest(msg,socket);
				break;
			case KILL:
				//kill all thread in the job
				//message are tasks
				killThread(msg,socket);
			default:
				break;
			}
			


		}
	}
	
	private void killThread(Message msg, Socket socket) {
		
		ArrayList<Task> tasks = (ArrayList<Task>) msg.getContent();
		for(Task task : tasks) {
			if(jobToThread.contains(task.getJobId())){
				for(Thread t : jobToThread.get(task.getJobId())){
					//kill thread?? not safe way.
					t.interrupt();
				}
			}else {
				continue;
			}
		}
	}

	private void fileGetContentRequest(Message msg, Socket socket) {
		String name = msg.getContent().toString();
		new FileTransfer.Upload("./"+name, socket).start();
	}

	/*
	 * download reduce files from other nodes
	 */
	private void downloadReduceResult(Message msg, Socket socket) {
		System.out.println("receive results from mapper");
		if(msg.getContent().toString() != null){
			new FileTransfer.Download(msg.getContent().toString(),socket,conf.ChunkSize).start();
		}
	}

	/*
	 * Handle reduce task
	 */
	private void newReduceHandler(Message msg, Socket socket) {
		Task task = (Task) msg.getContent();
		ReduceTracker performReduce = new ReduceTracker((Task)msg.getContent());  
		
		if(jobToThread.contains(task.getJobId())){
			jobToThread.get(task.getJobId()).add(performReduce);
		} else {
			jobToThread.put(task.getJobId(), new ArrayList<Thread>());
			jobToThread.get(task.getJobId()).add(performReduce);
		}
		performReduce.start();
	}
	
	/*
	 * Handle map task
	 * In msg, there should be reduce slave info. Maper slave can know where to send result.
	 */
	private void newMapHandler(Message msg, Socket socket) {
		Task task = (Task) msg.getContent();
		MapTracker performMap = new MapTracker((Task) msg.getContent()); 
		if(jobToThread.contains(task.getJobId())){
			jobToThread.get(task.getJobId()).add(performMap);
		} else {
			jobToThread.put(task.getJobId(), new ArrayList<Thread>());
			jobToThread.get(task.getJobId()).add(performMap);
		}
		performMap.start();
	}
	
	/*
	 * handle file download
	 */
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
			System.out.println("send the port number from the slave to the client"+SlaveMain.curPort);

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
