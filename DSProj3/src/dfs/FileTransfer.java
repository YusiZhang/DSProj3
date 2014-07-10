package dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/*
 * Handle file upload, download from node to node
 */
public class FileTransfer {

	// client side to server side
	// file existed locally
	public static class Upload extends Thread {
		String fileName = null;
		Socket socket = null;
		File myFile = null;
        byte[] mybytearray = null;
        FileInputStream fis = null;
        BufferedOutputStream outToClient = null;
        
        
		public Upload(String fileName, Socket socket) {
			this.fileName = fileName;
			this.socket = socket;
			
			
		}

		public void run() {
			System.out.println("File Downloading...");	
			try {
				myFile = new File(fileName);
				mybytearray = new byte[(int) myFile.length()];
				outToClient = new BufferedOutputStream(socket.getOutputStream());
				fis = new FileInputStream(myFile);
				BufferedInputStream bis = new BufferedInputStream(fis);
				bis.read(mybytearray, 0, mybytearray.length);
				outToClient.write(mybytearray, 0, mybytearray.length);
                outToClient.flush();
                outToClient.close();
			    this.socket.close();
			    System.out.println("Upload successed!");
			} catch (IOException e) {
				System.out.println("something is wrong with file uploading...");
				e.printStackTrace();
			}

		}
	}

	public static class Download extends Thread {
		//download from one slave
		String fileName = null;
		Socket socket = null;
		byte[] mybytearray = null;
		
		public Download(String fileName, Socket socket) {
			this.fileName = fileName;
			this.socket = socket;
			mybytearray = new byte[1024];
		}

		public void run() {
			InputStream is;
			try {
				is = this.socket.getInputStream();
				FileOutputStream fos = new FileOutputStream(fileName);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				int bytesRead = is.read(mybytearray, 0, mybytearray.length);
				bos.write(mybytearray, 0, bytesRead);
			    bos.close();
			} catch (IOException e) {
				System.out.println("something is wrong with file uploading...");
				e.printStackTrace();
			}
			
		}
	}

}
