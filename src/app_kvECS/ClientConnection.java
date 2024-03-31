package app_kvECS;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.*;



import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

import app_kvECS.ECSClient.ReplicaEventType;

//Facilitates the connection to servers (they act as clients to the ECS) 


public class ClientConnection implements Runnable{

    private static Logger logger = Logger.getRootLogger();
    
    private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    

    private Socket clientSocket;
	private InputStream input;
    private OutputStream output;
	private ECSClient ecsClient;

	

	private String serverAddress;
	private int serverPort;

	private String successor;
	private String successorAdd;
	private String replica1Addr;
	private String replica2Addr;
	private boolean shuttingDown;



    public ClientConnection(Socket clientSocket, ECSClient ecsClient){
        this.clientSocket = clientSocket;
        this.isOpen = true;
		this.ecsClient = ecsClient;
		this.shuttingDown = false;
	}
	
	public Socket getClientSocket(){
		return this.clientSocket;
	}

	public String getServerAddress(){
		return this.serverAddress;
	}

	public int getServerPort(){
		return this.serverPort;
	}

    public void run() {
		try {

			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			logger.info("Connection to server : " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort() + " established");
			
			while(isOpen) {
				try {
                    
					Message latestMsg = receiveMessage();

					if (latestMsg == null){
						logger.error("Server disconnected");
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
			logger.error("Error! Connection to server could not be established!", ioe);
			
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
				if (commandString == "" || requestString.trim().isEmpty()){ //disconnected from server
					return null; 
				}
				logger.error("Unknown command/Broken request : " + requestString);
				Message msg = new Message("error"," - unknown command/invalid format",KVMessage.StatusType.FAILED);
				return msg;
			}

			if (command == KVMessage.StatusType.REGISTER_SERVER || command == KVMessage.StatusType.DATA_TRANSFER  || command == KVMessage.StatusType.REPLICA_1 || command == KVMessage.StatusType.REPLICA_2){
				
				key = msgRequest[1];
				value = msgRequest[2];
			} else if ((command == KVMessage.StatusType.DATA_TRANSFER_START && this.shuttingDown == false) || command == KVMessage.StatusType.NEW_REPLICA_1 || command == KVMessage.StatusType.NEW_REPLICA_2 || command == KVMessage.StatusType.REPLICA_1_DEST || command == KVMessage.StatusType.REPLICA_2_DEST){
				key = msgRequest[1];
			}

			Message msg = new Message(key,value,command);

			return msg;
	}

	private void handleRequest(Message latestMessage){

		try{
			
			KVMessage.StatusType command = latestMessage.getStatus();

			switch (command){
				case SHUTDOWN:
					this.shuttingDown = true;
					if(this.ecsClient.getMetaDataSize() == 1){ //if last standing server 
						sendMessageSafe(new Message(null,null,KVMessage.StatusType.SHUTDOWN_LAST));
						this.isOpen = false;
					} else{
						sendMessageSafe(new Message(null,null,KVMessage.StatusType.SHUTDOWN));
					}
					handleServerShutdown();
					break;
				case REGISTER_SERVER:
					String serverAddress = latestMessage.getKey();
					String serverPort = latestMessage.getValue();
					handleRegistration(serverAddress,serverPort);
					break;
				case DATA_TRANSFER_START:
					if(this.shuttingDown){
						Message transferStart = new Message(null,null, KVMessage.StatusType.DATA_TRANSFER_START);
						this.ecsClient.sendToClient(this.successor, transferStart);
					} else{
						this.successorAdd = latestMessage.getKey();
						Message transferStart = new Message(null,null, KVMessage.StatusType.DATA_TRANSFER_START);
						this.ecsClient.sendToClient(this.successorAdd, transferStart);
					}
					break;
				case DATA_TRANSFER:
					String key = latestMessage.getKey();
					String value = latestMessage.getValue();

					logger.info("Data transfer Key:" + key + "Value:" + value);

					Message kv = new Message(key, value, KVMessage.StatusType.DATA_TRANSFER);
					if(this.shuttingDown){
						this.ecsClient.sendToClient(this.successor, kv);
					} else{
						this.ecsClient.sendToClient(this.successorAdd, kv);
					}
					break;
				case DATA_TRANSFER_COMPLETE:
					//update successors?
					Message transferComplete = new Message(null,null, KVMessage.StatusType.DATA_TRANSFER_COMPLETE);
					if(this.shuttingDown){
						this.ecsClient.sendToClient(this.successor, transferComplete);
						this.isOpen = false;
					} else{
						this.ecsClient.sendToClient(this.successorAdd, transferComplete);
					}
					break;
				case HEARTBEAT_REPLY:
					logger.info("Server" + this.serverAddress + ":" + this.serverPort +  "responded to heartbeat ping");
					//we don't have to do anything here
					break;
				case NEW_REPLICA_1:
					logger.info(latestMessage.getStringMessage());
					this.replica1Addr = latestMessage.getKey();
					logger.info("Replica 1 Destination:" + this.replica1Addr);
					Message newRep1Msg = new Message(null, null, KVMessage.StatusType.NEW_REPLICA_1);
					this.ecsClient.sendToClient(this.replica1Addr, newRep1Msg);
					break;
				case NEW_REPLICA_2:
					this.replica2Addr = latestMessage.getKey();
					Message newRep2Msg = new Message(null, null, KVMessage.StatusType.NEW_REPLICA_2);
					this.ecsClient.sendToClient(this.replica2Addr, newRep2Msg);
					break;
				case REPLICA_1_DEST:
					this.replica1Addr = latestMessage.getKey();
					break;
				case REPLICA_2_DEST:
					this.replica2Addr = latestMessage.getKey();
					break;
				case REPLICA_1:
					String rep1key = latestMessage.getKey();
					String rep1value = latestMessage.getValue();
					Message kvRep1 = new Message(rep1key, rep1value, KVMessage.StatusType.REPLICA_1);
					this.ecsClient.sendToClient(this.replica1Addr, kvRep1);
					break;
				case REPLICA_2:
					String rep2key = latestMessage.getKey();
					String rep2value = latestMessage.getValue();
					Message kvRep2 = new Message(rep2key, rep2value, KVMessage.StatusType.REPLICA_2);
					this.ecsClient.sendToClient(this.replica2Addr, kvRep2);
					break;
				default:
					sendMessageSafe(new Message("error",null,KVMessage.StatusType.FAILED)); //todo: return error string, technically it won't reach here
					break;
			}

		}
		catch(Exception e){
			sendMessageSafe(new Message("error",null,KVMessage.StatusType.FAILED));
		}
		

	}


	private void handleServerShutdown(){
		logger.info("Shut down handler called here");
		
		// Create a collection to store the concatenated address:port strings
        Collection<String> nodeNames = new ArrayList<>();


		String name = this.serverAddress + ":" + this.serverPort;
		nodeNames.add(name);

		//replica logic
		this.ecsClient.handleReplicaLogic(name ,ReplicaEventType.SERVER_SHUTDOWN);

		//logic of removing server
		this.ecsClient.removeNodes(nodeNames);
		this.successor = this.ecsClient.getDataTransferTarget();
	}


	private void handleRegistration(String serverAddress, String serverPort){

		this.serverAddress = serverAddress;
		this.serverPort =  Integer.parseInt(serverPort);;

		this.ecsClient.handleReplicaLogic(this.serverAddress+":"+this.serverPort, ReplicaEventType.SERVER_ADDED);
		this.ecsClient.addNode(this.serverAddress, this.serverPort,"default caching strat", 0);

	}



}
