package node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import communication.Message;
import communication.WriteFileMsg;
import config.ParseConfig;

public class Scheduler extends Thread{
	//slave pool
	public static ConcurrentHashMap<Integer,SlaveInfo> slavePool = new ConcurrentHashMap<Integer,SlaveInfo>();
	//file layout record key: slaveInfo value : fileName with blk id
	public static ConcurrentHashMap<String,ArrayList<SlaveInfo>> fileLayout = new ConcurrentHashMap<String,ArrayList<SlaveInfo>>();
	public static int slaveId = 0;
	ServerSocket listener;
	public Scheduler(int port) throws IOException{
		listener = new ServerSocket(port);
	}
	
	public void run() {
		
		while(true) {
			Socket socket = null;
			Message msg = null;
			ParseConfig conf = null;
			try {
				socket = listener.accept();
				conf = MasterMain.conf;
				msg = Message.receive(socket);
				System.out.println("the scheduler receives a "+msg.getType()+" messge");
				
			} catch (Exception e) {
				System.out.println("There is no message " + e.toString());
				continue;
			}
			
			switch (msg.getType()) {
			case REG_NEW_SLAVE:
				regNewSlaveHandler(socket);
				break;
				
			case FILE_PUT_REQ_TO_MASTER:
				
				for(int rep = 0; rep <= conf.Replica; rep++) {
					WriteFileMsg writeFileMsg = (WriteFileMsg) msg.getContent(); 
					ArrayList<SlaveInfo> slaveList = fileLayoutGenerate(slavePool,writeFileMsg);
					//console debug
					System.out.println(msg.getContent().toString() + "files");
					//generate file layout policy
					filePutReqToMasterHandler(socket,msg,slaveList);
				}
				break;
				
			
			
			default:
				break;
			}

		}
	}


	
	/*
	 * used for generate file replica policy
	 */
	/*
	private void fileReplicaHandler(WriteFileMsg writeFileMsg, int replica) {
		Random rng = new Random();
		String baseFileName = writeFileMsg.fileBaseName;
		for(int i = 0; i <= writeFileMsg.fileBlk;i++){
			int curRep = 0;
			while(curRep != MasterMain.conf.Replica){
				Integer next = rng.nextInt(slavePool.size());
				if(fileLayout.get(baseFileName + "_blk" +i).contains(slavePool.get(next))) {
					continue;
				}else {
					ArrayList<SlaveInfo> curSlaveList = fileLayout.get(writeFileMsg.fileBaseName + "_blk" + i);
					curSlaveList.add(slavePool.get(next));
					fileLayout.put(writeFileMsg.fileBaseName + "_blk" + i,curSlaveList);
					curRep++;
					break;
				}
			}
			
		}
		
	}
	*/

	/*
	 * Generate file layout policy and update fileLayout table.
	 * Notice, replica is not included
	 */
	private ArrayList<SlaveInfo> fileLayoutGenerate(ConcurrentHashMap<Integer, SlaveInfo> slavePool, WriteFileMsg writeFileMsg) {
		Random rng = new Random(); 
		Set<Integer> idSet = new HashSet<Integer>();
		//select random slave id for file to be input
		//======
		//trad-off: it maintains load balance, but when there is not enough slave nodes, the function will crush.
		//======
		while (idSet.size() < writeFileMsg.fileBlk)
		{
		    Integer next = rng.nextInt(slavePool.size());
		    if(slavePool.contains(next)){
		    	 idSet.add(next); //if slave is down, its slave ID will not be used for a while.
		    }
		   
		}
		ArrayList<SlaveInfo> slaveList = new ArrayList<SlaveInfo>();
		int fileId = 0;
		while(idSet.iterator().hasNext()) {
			int slaveId = idSet.iterator().next();
			SlaveInfo slaveInfo = slavePool.get(slaveId);
			slaveList.add(slaveInfo);
			if(fileLayout.contains(writeFileMsg.fileBaseName + "_blk" + fileId)) {
				ArrayList<SlaveInfo> curSlaveList = fileLayout.get(writeFileMsg.fileBaseName + "_blk" + fileId);
				curSlaveList.add(slaveInfo);
				fileLayout.put(writeFileMsg.fileBaseName + "_blk" + fileId,curSlaveList);
			}else {
				ArrayList<SlaveInfo> newSlaveList = new ArrayList<SlaveInfo>();
				newSlaveList.add(slaveInfo);
				fileLayout.put(writeFileMsg.fileBaseName + "_blk" + fileId,newSlaveList);
			}
			fileId++;
		}
		return slaveList;
	}

	/*
	 * reply client which slaves are to be connected to upload file
	 */
	private void filePutReqToMasterHandler(Socket socket, Message msg, ArrayList<SlaveInfo> slaveList) {
		System.out.println("Message received from " + socket.getRemoteSocketAddress() + " type: FILE_PUT_REQ_TO_MASTER; content: " + msg.getContent().toString());
		Message reply = new Message(Message.MSG_TYPE.AVAIL_SLAVES, slaveList);
		
		//tell the client which slaves are available
		try {
			
			reply.send(socket);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.println("send the slave list from the master to the client");
		System.out.println(slaveList.toString());
		
	}

	/*
	 * update slavePool when a new slave register on master
	 * @params socket
	 * 
	 */
	private void regNewSlaveHandler(Socket socket) {
		InetAddress address = socket.getInetAddress();
		SlaveInfo slave = new SlaveInfo(slaveId, address);
		slaveId++;
		System.out.println("connect to slave "+slave.slaveId+ " "+ address);
		slavePool.put(slaveId,slave);
		
	}


}
