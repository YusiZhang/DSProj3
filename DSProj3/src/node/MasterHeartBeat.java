package node;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import mapred.Task;
import communication.Message;
import config.ParseConfig;

public class MasterHeartBeat extends Thread {
	public void run() {

		System.out.println("start the heart beat thread on master");
		
		ParseConfig conf = MasterMain.conf;
		ArrayList<Task> killtasks = new ArrayList<Task>();
		while (true) {		
			synchronized (Scheduler.fileLayout) {
				synchronized (Scheduler.slavePool){
					for(String file : Scheduler.fileLayout.keySet()){
						Iterator it = Scheduler.fileLayout.get(file).iterator();
						while(it.hasNext()){
							SlaveInfo tempslave = (SlaveInfo) it.next();
							System.out.println("filename: " + file + "\t" + tempslave.slaveId);
						}
					}
				}
			}
			
			for (SlaveInfo slave : Scheduler.slavePool.values()) {
				try {			
					//send heartbeat msg
					Message msg = new Message(Message.MSG_TYPE.KEEP_ALIVE, "");
					Socket socket = new Socket(slave.address,conf.SlaveHeartBeatPort);
					msg.send(socket);
					msg = Message.receive(socket);
					if (!msg.getType().equals(Message.MSG_TYPE.KEEP_ALIVE)) {
						
						System.out.println("the slave " + slave.slaveId + " fails");
						throw new Exception();
						
					}
					
					//send kill tasks msg
					try{
						Message msgKill = new Message(Message.MSG_TYPE.KILL, killtasks);
						Socket socketKill = new Socket(slave.address,conf.SlaveMainPort);
						msgKill.send(socketKill);
						for(Task t : killtasks) {
							Socket socketRestart = new Socket(t.getAddress(),MasterMain.conf.ClientMainPort);
							
							Message jobFail = new Message(Message.MSG_TYPE.JOB_FAIL,t.getJobName()+"");
							jobFail.send(socketRestart);
							
						}
						killtasks.clear();
					} catch (Exception e){
					}
					
					
					
					
					
					
				} catch (Exception e) {
					System.out.println("fail to connect the z : " + slave.slaveId);

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
					
					if(Scheduler.SlaveToTask.get(Scheduler.slavePool.get(i)) == null){
					}else {
						killtasks.addAll(Scheduler.SlaveToTask.get(Scheduler.slavePool.get(i)));
					}
					
					
					
					Scheduler.slavePool.remove(i);
					
					
					
					synchronized (Scheduler.fileLayout) {
						synchronized (Scheduler.slavePool){
							for(String file : Scheduler.fileLayout.keySet()){
								Iterator it = Scheduler.fileLayout.get(file).iterator();
								while(it.hasNext()){
									SlaveInfo slave = (SlaveInfo) it.next();
									if(slave.slaveId == i) {
										it.remove();
									}
								}
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
