package dfs;

import java.net.Socket;

/*
 * Handle file upload, download from node to node
 */
public class FileTransfer {
	
	public static class Upload extends Thread {
		String fileName = null;
		Socket socket = null;

		public Upload(String fileName, Socket socket) {
			this.fileName = fileName;
			this.socket = socket;
		}

		public void run() {
			System.out.println("File Downloading...");
			while(true) {
				
			}
		}
	}

	public static class Download extends Thread {
		String fileName = null;
		Socket socket = null;

		public Download(String fileName, Socket socket) {
			this.fileName = fileName;
			this.socket = socket;
		}

		public void run() {

		}
	}

}
