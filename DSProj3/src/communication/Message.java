package communication;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * Message used between nodes
 */
public class Message implements Serializable{
	public static enum MSG_TYPE {
	
		FILE_PUT_REQ_TO_MASTER, FILE_PUT_REQ_TO_SLAVE, FILE_PUT_ACK,FILE_PUT_DONE, FILE_PUT_FAIL,
		FILE_GET_REQ, FILE_GET_ACK,FILE_GET_DONE, FILE_GET_FAIL,
		REG_NEW_SLAVE,AVAIL_SLAVES,PORT,
		FILE_PUT_START_TO_SLAVE,
		KEEP_ALIVE, NEW_JOB, JOB_COMP, JOB_FAIL, NEW_TASK, MAPPER_SUCCESS
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
	
	/*
	 * send this message to remote host
	 * @param resuedSocket
	 */
	public void send(Socket soc) throws Exception{
		Socket socket = soc;
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(this);
		out.flush();
	}
	
	/*
	 * receive message from remote host
	 */
	public static Message receive(Socket soc) throws ClassNotFoundException, Exception {
		Message msg = null;
		Socket socket = soc;
		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
		msg = (Message)in.readObject();
		return msg;
	}

}
