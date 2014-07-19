package communication;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;

/**
 * Message used between nodes
 */
public class Message implements Serializable {
	public static enum MSG_TYPE {

		FILE_PUT_REQ_TO_MASTER, FILE_PUT_REQ_TO_SLAVE, FILE_PUT_DONE, FILE_PUT_FAIL, FILE_GET_REQ, FILE_GET_DONE, FILE_GET_FAIL, REG_NEW_SLAVE, AVAIL_SLAVES, PORT, FILE_PUT_START_TO_SLAVE, KEEP_ALIVE, NEW_JOB, JOB_COMP, JOB_FAIL, NEW_MAP_TO_SLAVE, NEW_REDUCE_TO_SLAVE, MAPPER_SUCCESS, MAPPER_DONE, REDUCER_DONE, MAPRESULT_TO_REDUCE, NEW_MAPPER, NEW_REDUCER, GETFILE, FILEDOWNLOAD, KILL
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
	 * 
	 * @param resuedSocket
	 */
	public void send(Socket soc) throws Exception {
		// Socket socket = soc;
		ObjectOutputStream out = new ObjectOutputStream(soc.getOutputStream());
		out.writeObject(this);
		out.flush();
		System.out.println("Sending " + this.getType() + " / "
				+ this.getContent().toString() + " to "
				+ soc.getRemoteSocketAddress());
	}

	/*
	 * receive message from remote host
	 */
	public static Message receive(Socket soc) throws ClassNotFoundException,
			Exception {
		Message msg = null;
		// Socket socket = soc;
		ObjectInputStream in = new ObjectInputStream(soc.getInputStream());
		msg = (Message) in.readObject();
		System.out.println("Receiving " + msg.getType() + " / "
				+ msg.getContent().toString() + " from "
				+ soc.getRemoteSocketAddress());
		return msg;
	}

}
