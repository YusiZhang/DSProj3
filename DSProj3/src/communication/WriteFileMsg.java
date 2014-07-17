package communication;

import java.io.Serializable;
/*
 * This message used for client sends file upload req to master
 * And maintance filelayout table.
 */
public class WriteFileMsg implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2480900280335261115L;
	public String fileBaseName;
	public int fileBlk;
	public String fileName;
	public WriteFileMsg(String baseName,int fileBlk) {
		this.fileBaseName = baseName;
		this.fileBlk = fileBlk;
	}	
}
