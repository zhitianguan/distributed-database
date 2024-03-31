package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.*;

import shared.messages.KVMessage;


import app_kvClient.KVClient;
import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;




/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket,KVServer kvServer) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.kvServer = kvServer;

		//registerShutdownHook();	//uncomment this if disconnecting client is not behaving properly 
	}

	private void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info("Killing client connection :  " + clientSocket.getInetAddress().getHostAddress());
				disconnect();
			}
		}));
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {

			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			logger.info("Connection to MSRG Echo server established: " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort());
			
			while(isOpen) {
				try {
					Message latestMsg = receiveMessage();

					if (latestMsg == null){
						logger.error("Client disconnected");
						isOpen = false;
					}		
					else if (latestMsg.getStatus() == KVMessage.StatusType.FAILED) {	
						sendMessage(latestMsg);
					}		
					else {
						handleRequest(latestMsg);
					}
										
				
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	/**
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(Message msg) throws IOException {
		String messageResponse = msg.getStringMessage();
		byte[] msgBytes = msg.toByteArray(messageResponse);
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getStringMessage() +"'");
	}
	

	public void sendMessageSafe(Message msg){ //this is so that we don't always have to do try catch whenever calling sendMessage
		try{
			sendMessage(msg);
		}
		catch(Exception e){
			logger.error("Failed to send message : " + e.toString());
		}
	}
	


	public void disconnect() {
		isOpen = false;
		try {
			if (input != null) {
				input.close();
			}
			if (output != null) {
				output.close();
			}
			if (clientSocket != null) {
				clientSocket.close();
			}
			logger.info("Disconnected client connection for: " + clientSocket.getInetAddress().getHostAddress());
		} catch (IOException e) {
			logger.error("Error closing the client connection: ", e);
		}	
	}
	
	private Message receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		String requestString = new String(msgBytes, StandardCharsets.UTF_8);
		Message msg = convertStringToMessage(requestString);

		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ requestString + "'");
		return msg;
	}
	


	private Message convertStringToMessage(String requestString){
			String[] msgRequest = requestString.trim().split(" ");
			KVMessage.StatusType command = null;
			String key = null;
			String value = null;


			if (msgRequest.length < 1){
				//sendMessageSafe(new TextMessage("Command not found"));
				logger.error("Command not found");
				return null;
			}

			String commandString = msgRequest[0].toUpperCase(); //status

			try{
				command = KVMessage.StatusType.valueOf(commandString);
			}
			catch(IllegalArgumentException e){
				logger.error("Unknown command/Broken request");
				if (commandString == ""){ //client wants to disconnect
					return null; 
				}
				Message msg = new Message("error"," - unknown command/invalid format",KVMessage.StatusType.FAILED);
				return msg;
			}

			if (msgRequest.length > 1){
				key = msgRequest[1];
			}

			if (command == KVMessage.StatusType.PUT && msgRequest.length > 2 && msgRequest[2] != null){
				value = String.join(" ", Arrays.copyOfRange(msgRequest,2,msgRequest.length));
			}

			Message msg = new Message(key,value,command);

			return msg;
	}

	private void handleRequest(Message latestMessage){
		//request format: <command> <requestData> 
		try{
			KVMessage.StatusType command = latestMessage.getStatus();
			String key = latestMessage.getKey();
			if (key == null && (command != KVMessage.StatusType.KEYRANGE || command != KVMessage.StatusType.KEYRANGE_READ) ){
				sendMessageSafe(new Message("error","- key not provided",KVMessage.StatusType.FAILED)); //INVALID_PARAMETER
				return;
			}
			switch (command){
				case GET :
					handleGet(key);
					break;
				case PUT :
					String value = latestMessage.getValue();
					if (this.kvServer.getWriteLock() == 0){ // check if server is writeLocked due to a data transfer
						handlePut(key,value);
					} else{
						sendMessageSafe(new Message(key,value,KVMessage.StatusType.SERVER_WRITE_LOCK));
					}
					break;
				case KEYRANGE :
					sendMessageSafe(new Message(kvServer.metadataToString(),"\r\n", KVMessage.StatusType.KEYRANGE_SUCCESS));
					break;
				case KEYRANGE_READ:
					sendMessageSafe(new Message(kvServer.metadataToStringForKeyRange(true),"\r\n", KVMessage.StatusType.KEYRANGE_READ_SUCCESS));
					break;
				default:
					sendMessageSafe(new Message("error",null,KVMessage.StatusType.FAILED)); //todo: return error string, technically it won't reach here
				break;
			}

		}
		catch(Exception e){
			sendMessageSafe(new Message("error",null,KVMessage.StatusType.FAILED)); //todo: return error string;
		}
		

	}

	private void handleGet(String key){

		if (this.kvServer.isConnectedToECS() && !this.kvServer.isInRange(key)){
			sendMessageSafe(new Message(this.kvServer.metadataToString(), null, KVMessage.StatusType.SERVER_NOT_RESPONSIBLE));
			return; //exit
		}

		Boolean keyFound = this.kvServer.inStorage(key);


		if (!keyFound){
			sendMessageSafe(new Message(key,null,KVMessage.StatusType.GET_ERROR));
			logger.info("Key :" + key + " value not found");
		}
		else{
			try{
				String value = this.kvServer.getKV(key);
				logger.info("Got value for key : " + key + " value is :  " + value);
				sendMessageSafe(new Message(key,value,KVMessage.StatusType.GET_SUCCESS));

			}
			catch(Exception e){
				sendMessageSafe(new Message(key,null,KVMessage.StatusType.GET_ERROR));
				logger.error("Failed getting value for key : " + key + e.getMessage());

			}
		}

	}

	private void handlePut(String key, String value){

		if (this.kvServer.isConnectedToECS() && !this.kvServer.isInRange(key)){
			sendMessageSafe(new Message(this.kvServer.metadataToString(), null, KVMessage.StatusType.SERVER_NOT_RESPONSIBLE));
			return; //exit
		}

		
		Boolean keyFound = this.kvServer.inStorage(key);

		if (value == null){ //COMPLETE THIS
			try{
				if (keyFound){
					this.kvServer.deleteKV(key);
					logger.info("Removed key:" + key);
					sendMessageSafe(new Message(key,null,KVMessage.StatusType.DELETE_SUCCESS)); 
					this.kvServer.newKVPut = true;
					this.kvServer.newKVKey.add(key);
					this.kvServer.newKVValue.add(null);
				} else{
					logger.info("Key :" + key + " value not found");
					sendMessageSafe(new Message(key,null,KVMessage.StatusType.DELETE_ERROR)); 
				}
			}
			catch(Exception e){
				sendMessageSafe(new Message(key,null,KVMessage.StatusType.DELETE_ERROR));
				logger.error("Failed to remove key :" + key + " - " + e.getMessage());
			}
			
			return;
		}

		

		try{
			this.kvServer.putKV(key,value);
			this.kvServer.newKVPut = true;
			this.kvServer.newKVKey.add(key);
			this.kvServer.newKVValue.add(value);
			if (keyFound){ //update
				sendMessageSafe(new Message(key,value,KVMessage.StatusType.PUT_UPDATE));
				logger.info("Updated value of key :" + key + " to " + value);
			}
			else{
				sendMessageSafe(new Message(key,value,KVMessage.StatusType.PUT_SUCCESS));
			}
		}
		catch(Exception e){
			sendMessageSafe(new Message(key,value,KVMessage.StatusType.PUT_ERROR));
			logger.error("Failed to put key :" + key + " - " + e.getMessage());
		}


	}
	


}

