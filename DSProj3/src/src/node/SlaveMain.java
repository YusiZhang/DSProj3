package node;

import java.net.Socket;

import communication.Message;

import config.ParseConfig;

/*
 * For dfs, slave should do following things:
 * 1. response heartbeat request from master
 * 2. receive file block from client.
 * 3. send file block to client
 * 4. send replica to other slaves
 */
public class SlaveMain {
	static int curPort;
	static ParseConfig conf;
	public static void main(String[] args) {
		
		
		try {
			System.out.println("start slave");
			conf = new ParseConfig(args[0]);
			curPort = conf.StartPort;
			Socket socket = new Socket(conf.MasterIP, conf.MasterMainPort);
			//keep listener alive
			SlaveScheduler scheduler = new SlaveScheduler(conf.SlaveMainPort);
			scheduler.start();
			
			Message msg = new Message(Message.MSG_TYPE.REG_NEW_SLAVE, "I am a new slave");
			try {
				msg.send(socket);
//				System.out.println(msg.receive(socket).getContent());
				System.out.println("send msg from slave");
			} catch (Exception e) {
				System.out.println("Some wrong with put message " + e.toString());
			}
			
			SlaveScheduler slaveHeartBeat = new SlaveScheduler(conf.SlaveHeartBeatPort);
			slaveHeartBeat.start();
//			System.out.println("here goes the slave");
//			Scheduler scheduler1 = new Scheduler(curPort);
//			System.out.println("listening on the cur port:"+curPort);
//			scheduler1.start();
			
//			socket.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		String curPort = ParseConfig.StartPort;
	}
}
