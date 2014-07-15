import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import dfs.FileTransfer;


public class Test_Server {
	public static void main(String[] args) {
		try {
			ServerSocket listener = new ServerSocket(15440);
			Socket fileSoc = listener.accept();
			System.out.println("listening..");
			new FileTransfer.Download("src/slaveCopy1.txt", fileSoc, 6543).start();;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
