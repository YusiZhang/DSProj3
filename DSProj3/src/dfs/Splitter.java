package dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/*
 * split file into chunks and save them on client mechine 
 */
public class Splitter {

	private String baseFileName;
	private long chunkSize;
	private String savePath;
	public int fileBlk;

	public Splitter(String baseFileName, long chunkSize, String savePath) {
		this.baseFileName = baseFileName;
		this.chunkSize = chunkSize;
		this.savePath = savePath;
	}

	public void split() throws Exception {
		//everytime call split method, fileBlk should set to 0; 
		fileBlk = 0;
		// open base file
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(
				this.baseFileName));

		// calculate size of file
		File file = new File(this.baseFileName);
		long fileSize = file.length();

		// split file into each full chunk
		
		int fileNum = (int) (fileSize / this.chunkSize);
		for (fileBlk = 0; fileBlk < fileNum; fileBlk++) {
			BufferedOutputStream out = new BufferedOutputStream(
					new FileOutputStream(baseFileName + "_blk" + fileBlk));
			// write loop
			for (int curByte = 0; curByte < chunkSize; curByte++) {
				// write from read
				out.write(in.read());
			}
			// close
			out.close();
		}
		// last chunk may smaller then chunkSize
		if (fileSize != chunkSize * (fileNum - 1)) {
			// open the output file
			BufferedOutputStream out = new BufferedOutputStream(
					new FileOutputStream(baseFileName + "_blk" + fileBlk));

			// write the rest of the file
			int b;
			while ((b = in.read()) != -1)
				out.write(b);

			// close writing the file
			out.close();
		}
		
		//close read the file
		in.close();
	}
	
	/*
	public static void main(String[] args) {
		new Splitter("src/harrypotter.txt", 4194304L, "");
	}
	*/
}
