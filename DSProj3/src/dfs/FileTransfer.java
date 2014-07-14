 package dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
			System.out.println("File Uploading...");	
			try {
				
				System.out.println("Iamhere");	
				myFile = new File(fileName);
				mybytearray = new byte[(int) myFile.length()];
				System.out.println(mybytearray);
				outToClient = new BufferedOutputStream(socket.getOutputStream());
				System.out.println("getoutputstream");
				fis = new FileInputStream(myFile);
				BufferedInputStream bis = new BufferedInputStream(fis);
				bis.read(mybytearray, 0, mybytearray.length);
				System.out.println(bis.toString());
				outToClient.write(mybytearray, 0, mybytearray.length);
				System.out.println("write successed!");
                outToClient.flush();
                outToClient.close();
			    this.socket.close();
			    System.out.println("Upload successed!");
			    
				
				/*
				DataInputStream file = null;
		        DataOutputStream sockdata = null;
		        try {
		            file = new DataInputStream(new BufferedInputStream(
		                    new FileInputStream(fileName)));
		            sockdata = new DataOutputStream(socket.getOutputStream());
		            byte[] buf = new byte[Constants.BufferSize];
		            int read_num;
		            while ((read_num = file.read(buf)) != -1) {
		                sockdata.write(buf, 0, read_num);
		            }
		        } catch (FileNotFoundException e) {
		            System.out.println("The file does not exist");
		            byte[] buf = new byte[Constants.BufferSize];
		            sockdata.write(buf, 0, 0);
		        }

		        sockdata.flush();
		        file.close();
				*/
				
				
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
			mybytearray = new byte[8196000];
		}

		public void run() {
			InputStream is;
			try {
				
				is = this.socket.getInputStream();
				FileOutputStream fos = new FileOutputStream(fileName);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				int bytesRead = is.read(mybytearray, 0, mybytearray.length);
				System.out.println(bytesRead);
				bos.write(mybytearray, 0, bytesRead);
			    bos.close();
			} catch (IOException e) {
				System.out.println("something is wrong with file downloading...");
				e.printStackTrace();
			}
			
		}
	}

}
