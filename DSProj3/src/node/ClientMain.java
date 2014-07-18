package node;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Scanner;

import communication.Message;
import communication.Message.MSG_TYPE;
import communication.WriteFileMsg;
import config.ParseConfig;
import dfs.FileTransfer;
import dfs.Splitter;

/*
 * for dfs:
 * 1. upload file
 * 2. split file
 * 3. get file
 */
public class ClientMain{
	public static enum CMD {

		put,get

	}

	public static void main(String[] args) {

		CMD cmd_type = null;

		// connect to master.
		Socket socket = null;
		try {
			new ParseConfig(args[0]);
			socket = new Socket(ParseConfig.MasterIP,
					ParseConfig.MasterMainPort);
		} catch (Exception e) {
			System.out.println(e.toString());
		}

		Scanner scanner = new Scanner(new InputStreamReader(System.in));
		System.out.println("Enter your cmd ");
		String completecmd = scanner.nextLine();
		String cmd = completecmd.split("-")[0];
		String param = completecmd.split("-")[1];
		// accept cmd from console
		switch (CMD.valueOf(cmd)) {
		case put:
			putFileHandler(socket, param);

			break;
		case get:
			System.out.println("Please enter file name");
			String name = scanner.nextLine();
			System.out.println("Please enter slave id");
			String slaveid = scanner.nextLine();
			
			SlaveInfo slave = getFileLocation(name, slaveid);
			if(slave != null) {
				getFileContent(name,slave);
			}else {
				System.out.println("file is not exited on slave id : " + slaveid);
			}
		default:
			break;
		}

	}
	
	private static void getFileContent(String name, SlaveInfo slave) {
		try {
			Socket socket = new Socket(slave.address,ParseConfig.SlaveMainPort);
			Message requestDownload = new Message(MSG_TYPE.FILEDOWNLOAD,name);
			requestDownload.send(socket);
			Thread.sleep(500);
			new FileTransfer.Download(name, socket, ParseConfig.ChunkSize).start();
			BufferedReader br = new BufferedReader(new FileReader("./"+name));
			String line;
			int count = 0;
			System.out.println("Here are first 5 lines of file" + name);
			while ((line = br.readLine()) != null) {
			   System.out.println(line);
			   count++;
			   if(count > 5) {
				   break;
			   }
			}
			br.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static SlaveInfo getFileLocation(String name, String slaveid) {
		try {
			
			Socket socket = new Socket(ParseConfig.MasterIP,ParseConfig.MasterMainPort);
			FileInfo info = new FileInfo();
			info.fileName = name;
			info.slaveInfo = new SlaveInfo(0, null);
			info.slaveInfo.slaveId = Integer.parseInt(slaveid);
			Message getFileMsg = new Message(MSG_TYPE.GETFILE,info);
			getFileMsg.send(socket);
			Message slaveMsg = getFileMsg.receive(socket);
			SlaveInfo slave = (SlaveInfo) slaveMsg.getContent();
			System.out.println("file : " + name + " is on slave id: " + slave.slaveId + "IP: " + slave.address );
			
			return slave;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void putFileHandler(Socket socket, String fileName) {

		try {
			// split file
			Splitter splitter = new Splitter(fileName, ParseConfig.ChunkSize,
					"");
			splitter.split();
			// get file blk nums
			int blkNums = splitter.fileBlk;
			int count = 0;
			WriteFileMsg writeFileMsg = new WriteFileMsg(fileName, blkNums);
			Message msg = new Message(Message.MSG_TYPE.FILE_PUT_REQ_TO_MASTER,
					writeFileMsg);
			msg.send(socket);

			// get the slave list
			msg = Message.receive(socket);
			System.out.println("the client receives a message from the master");
			//close socket connecting with master
			socket.close();
			
			
			ArrayList<SlaveInfo> slaveList = (ArrayList<SlaveInfo>) msg.getContent();

			// connect the slaves via socket
			for (SlaveInfo s : slaveList) {
				if (count <= blkNums) {
					InetAddress add = s.address;

					System.out.println("the client will connect:" + add + " "
							+ ParseConfig.SlaveMainPort);
					Socket socketToSlave = new Socket(add, ParseConfig.SlaveMainPort);

					msg = new Message(Message.MSG_TYPE.FILE_PUT_REQ_TO_SLAVE,"I will upload a file to the slave");
					System.out.println("send the msg: " + msg.getContent());
					msg.send(socketToSlave);
					System.out.println("client send the msg to slave");

					msg = Message.receive(socketToSlave);// waiting to receive what
													// port should be uploding
													// file to slave. msg sent
													// from slave
				
					//close socket connecting slave main port
					socketToSlave.close();
					System.out.println("the client receives a message from the slave");
					// connect the slave via the port assigned by the slave
					Socket socketPutFile = new Socket(add, Integer.parseInt(msg.getContent().toString()));
					System.out.println("client connect to slave via assigned port "+ msg.getContent().toString());

					// Notice!!! this msg is used for test... need to change the
					// MSG Type!
					msg = new Message(Message.MSG_TYPE.FILE_PUT_START_TO_SLAVE,
							fileName + "_blk" + count);
					msg.send(socketPutFile);
					Thread.sleep(500);

					new FileTransfer.Upload(fileName + "_blk" + count, socketPutFile).start();
					count++;
					System.out.println("Uploaded " + fileName + "_blk" + count);
					
				} else {
					//set count = 0 for replica
					count = 0;
					InetAddress add = s.address;

					System.out.println("the client will connect:" + add + " "
							+ ParseConfig.SlaveMainPort);
					Socket socketToSlave = new Socket(add, ParseConfig.SlaveMainPort);

					msg = new Message(Message.MSG_TYPE.FILE_PUT_REQ_TO_SLAVE,
							"I will upload a file to the slave");
					System.out.println("send the msg: " + msg.getContent());
					msg.send(socketToSlave);
					System.out.println("client send the msg to slave");

					msg = Message.receive(socketToSlave);// waiting to receive what
													// port should be uploding
													// file to slave. msg sent
													// from slave
				
					//close socket connecting slave main port
					socketToSlave.close();
					System.out.println("the client receives a message from the slave");
					// connect the slave via the port assigned by the slave
					Socket socketPutFile = new Socket(add, Integer.parseInt(msg.getContent().toString()));
					System.out.println("client connect to slave via assigned port "+ msg.getContent().toString());

					// Notice!!! this msg is used for test... need to change the
					// MSG Type!
					msg = new Message(Message.MSG_TYPE.FILE_PUT_START_TO_SLAVE,
							fileName + "_blk" + count);
					msg.send(socketPutFile);
					Thread.sleep(500);

					new FileTransfer.Upload(fileName + "_blk" + count, socketPutFile).start();
					count++;
					System.out.println("Uploaded " + fileName + "_blk" + count);
				}

			}

		} catch (Exception e) {
			System.out.println("Some wrong with put message " + e.toString());
			e.printStackTrace();
			
		}

	}

}

/*
 * package node;
 * 
 * import java.io.Console; import java.io.IOException; import
 * java.io.InputStreamReader; import java.net.InetAddress; import
 * java.net.InetSocketAddress; import java.net.Socket; import
 * java.net.UnknownHostException; import java.util.ArrayList; import
 * java.util.Scanner;
 * 
 * import communication.Message; import config.ParseConfig;
 * 
 * /* for dfs: 1. upload file 2. split file 3. get file
 * 
 * public class ClientMain { public static enum CMD {
 * 
 * put
 * 
 * } public static void main(String[] args) {
 * System.out.println("start the client"); CMD cmd_type = null; //connect to
 * master. Socket socket = null; try { socket = new Socket(ParseConfig.MasterIP,
 * ParseConfig.MasterMainPort); } catch (Exception e) {
 * System.out.println(e.toString()); }
 * 
 * /* Scanner scanner = new Scanner(new InputStreamReader(System.in));
 * System.out.println("Enter your cmd "); String cmd = scanner.nextLine();
 * 
 * //accept cmd from console switch (CMD.valueOf(cmd)) { case put: Message msg =
 * new Message(Message.MSG_TYPE.FILE_PUT_REQ_TO_MASTER,
 * "I will upload a file later"); try { msg.send(socket); //
 * System.out.println(msg.receive(socket).getContent().toString());
 * 
 * //get the slave list msg = Message.receive(socket);
 * System.out.println("the client receives a message from the master");
 * ArrayList<SlaveInfo> slaveList = (ArrayList<SlaveInfo>) msg.getContent();
 * //connect the slaves via socket for (SlaveInfo s: slaveList) { InetAddress
 * add = s.address; System.out.println("the client will connect:"
 * +add.toString()+": "+ParseConfig.SlaveMainPort); socket = new Socket(add,
 * ParseConfig.SlaveMainPort);
 * 
 * msg = new Message(Message.MSG_TYPE.FILE_PUT_REQ_TO_SLAVE,
 * "I will upload a file to the slave");
 * System.out.println("client try to connect slave"); msg.send(socket);
 * 
 * msg = Message.receive(socket);
 * System.out.println("the client receives the msg from slave: connect port" +
 * msg.getContent());
 * 
 * } } catch (Exception e) { System.out.println("Some wrong with put message " +
 * e.toString()); } break;
 * 
 * default: break; }
 * 
 * }
 * 
 * }
 */
