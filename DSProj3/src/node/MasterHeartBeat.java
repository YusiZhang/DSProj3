package node;

import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import mapred.Task;
import communication.Message;
import config.ParseConfig;

public class MasterHeartBeat extends Thread {
	public void run() {
//		try {
//			Thread.sleep(5000);
//		} catch (InterruptedException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		System.out.println("start the heart beat thread on master");
		
		ParseConfig conf = MasterMain.conf;
		ArrayList<Task> killtasks = new ArrayList<Task>();
		while (true) {		
			System.out.println("entering true loop!!");
			System.out.println("slavePool size " + Scheduler.slavePool.size());
			for (SlaveInfo slave : Scheduler.slavePool.values()) {
				try {
					System.out.println("cur slave is " + slave.slaveId);
					
					//send kill tasks msg
					Message msgKill = new Message(Message.MSG_TYPE.KILL, killtasks);
					Socket socketKill = new Socket(slave.address,conf.SlaveMainPort);
					msgKill.send(socketKill);
					System.out.println("send kill message");
					for(Task t : killtasks) {
						Socket socketRestart = new Socket(t.getAddress(),MasterMain.conf.ClientMainPort);
						Message jobFail = new Message(Message.MSG_TYPE.JOB_FAIL,t.getJobName());
						jobFail.send(socketRestart);
					}
					killtasks.clear();
					
					//send heartbeat msg
					Message msg = new Message(Message.MSG_TYPE.KEEP_ALIVE, "");
					Socket socket = new Socket(slave.address,conf.SlaveHeartBeatPort);
					System.out.println("try to connect slave and send KEEP_ALIVE msg");
					msg.send(socket);
					System.out.println("send heart beat to slave");
					msg = Message.receive(socket);
					if (!msg.getType().equals(Message.MSG_TYPE.KEEP_ALIVE)) {
						
						System.out.println("the slave " + slave.slaveId + " fails");
						throw new Exception();
						
					}
				} catch (Exception e) {
					System.out.println("fail to connect the slave : " + slave.slaveId);

					Scheduler.failPool.put(slave.slaveId, slave);
					e.printStackTrace();
					continue;
//					
				}
				
			}
			if (Scheduler.failPool.size() > 0) {
				//remove the slaves from the master's scheduler
				
				
				//remove from slavepool
				for (int i: Scheduler.failPool.keySet()) {
					
					killtasks.addAll(Scheduler.SlaveToTask.get(Scheduler.slavePool.get(i)));
					
					Scheduler.slavePool.remove(i);
					
					
					
					for(String file : Scheduler.fileLayout.keySet()){
						for(SlaveInfo slave : Scheduler.fileLayout.get(file)){
							if(slave.slaveId == i) {
								//remove from file layout table
								Scheduler.fileLayout.get(file).remove(slave);
								
							}
						}
					}
				}
				
				
				
				
				Scheduler.failPool.clear();
				
			}
			
			try {
				sleep(conf.HearBeatFreq);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}



	
}
