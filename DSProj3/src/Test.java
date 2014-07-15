import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import config.ParseConfig;
import dfs.FileTransfer;
import dfs.Splitter;


public class Test {
	public static void main(String[] args) {
//		try {
//			ParseConfig config = new ParseConfig("src/ConfigFile.txt");
//			System.out.println(config.MasterIP+"");
//			System.out.println(config.MasterMainPort+"");
//			System.out.println(config.StartPort+"");
//			System.out.println(config.EndPort+"");
//		} catch (Exception e) {
//			
//			System.out.println("Something wrong with config file" + "\n" + e.toString());
//			System.exit(1);
//		}
		
		
		/*
		try {
			Splitter splitter = new Splitter("src/harrypotter.txt", 5000, "");
			splitter.split();
			//out put the last chunk num of the file.
			System.out.println(splitter.fileBlk);
			
			
		} catch (Exception e) {
			
			System.out.println("" + "\n" + e.toString());
		}
		*/
		
		/*
		//test as a client
		try {
			Socket socket = new Socket("127.0.0.1",15440);
			new FileTransfer.Upload("src/harrypotter.txt", socket).start();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		String str1 = "1";
		String str2 = "2";
		String str3 = "3";
		ArrayList<String> list = new ArrayList<String>();
		list.add(str2);
		list.add(str1);
		list.add(str3);
		for(String str : list){
			System.out.println(str);
		}
	}
}
