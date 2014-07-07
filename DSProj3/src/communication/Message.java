package communication;

import java.io.Serializable;

/**
 * Message used between nodes
 */
public class Message implements Serializable{
	public static enum MSG_TYPE {
	
		FILE_PUT_REQ, FILE_PUT_DONE, FILE_PUT_FAIL,
		FILE_GET_REQ, FILE_GET_DONE, FILE_GET_FAIL,
		
	}
	
	private MSG_TYPE type;
	private Serializable content;
	
	public Message(MSG_TYPE type, Serializable content) {
		this.type = type;
		this.content = content;
	}

	public MSG_TYPE getType() {
		return type;
	}

	public void setType(MSG_TYPE type) {
		this.type = type;
	}

	public Serializable getContent() {
		return content;
	}

	public void setContent(Serializable content) {
		this.content = content;
	}
	
	

}
