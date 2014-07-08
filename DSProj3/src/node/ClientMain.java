package node;

import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import communication.Message;
import config.ParseConfig;

/*
 * for dfs:
 * 1. upload file
 * 2. split file
 * 3. get file
 */
public class ClientMain {
	public static enum CMD {
		
		put
		
	}
	public static void main(String[] args) {
		
		CMD cmd_type = null;
		
		//connect to master.
		Socket socket = null;
		try {
        	new ParseConfig(args[0]);
			socket = new Socket(ParseConfig.MasterIP, ParseConfig.MasterMainPort);
		} catch (Exception e) {
			System.out.println(e.toString());
		} 
        
		
		Scanner scanner = new Scanner(new InputStreamReader(System.in));
		System.out.println("Enter your cmd ");
		String cmd = scanner.nextLine();
		
        //accept cmd from console
        switch (CMD.valueOf(cmd)) {
		case put:
			Message msg = new Message(Message.MSG_TYPE.FILE_PUT_REQ, "I will upload a file later");
			try {
				msg.send(socket);
				System.out.println(msg.receive(socket).getContent());
			} catch (Exception e) {
				System.out.println("Some wrong with put message " + e.toString());
			}
			break;

		default:
			break;
		}
		
	}
	
}
