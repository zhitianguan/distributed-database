package app_kvServer;


import app_kvServer.KVServer;
import app_kvServer.KVServer.SocketStatus;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.MissingFormatWidthException;
import java.util.Set;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

import java.util.concurrent.CountDownLatch;
import ecs.IECSNode;
import ecs.ECSNode;

import java.util.Map;
import java.util.TreeMap;
import java.math.BigInteger;

import app_kvServer.KVServer;

//connects to ECS or another Server
public class ConnectionManager extends Thread {

	private Logger logger = Logger.getRootLogger();
	//private Set<KVServer> listeners;
	private boolean running;
	
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	private String targetServerAddress;
	private int targetServerPort;
	private KVServer KVServerInstance;
	private String dataTransferTarget;
	private boolean shuttingDown;
	private int prevNumServers;
	private boolean updateAllReplicas;


	public ConnectionManager(String address, int port, KVServer instance) {
		this.targetServerAddress = address;
		this.targetServerPort = port;
		this.KVServerInstance = instance;
		this.dataTransferTarget = "";
		this.shuttingDown = false;
		this.prevNumServers = 0;
		this.updateAllReplicas = false;
		//listeners = new HashSet<KVServer>();
		Runnable shutdownTask = new Runnable() {
			@Override
				public void run() {
					System.out.println("Shutting down server");
					shutdownServer();
					disconnect();
				}
			};
	
		Thread shutdownThread = new Thread(shutdownTask);
		Runtime.getRuntime().addShutdownHook(shutdownThread);
	}

	public synchronized void disconnect() {
		logger.info("try to close connection to " + targetServerAddress + " : " + targetServerPort);
		try {
			tearDownConnection();
			//for(KVServer listener : listeners) {
			//	listener.handleStatus(SocketStatus.DISCONNECTED);
			//}
			handleStatus(SocketStatus.DISCONNECTED);
		} catch (IOException ioe) {
			logger.error("Unable to close connection to " + targetServerAddress + " : " + targetServerPort);
		}
		setRunning(false);
	}
	
	public void createAndStartNewConnection(String newTargetAddress, int newTargetPort) { //to connect to other server if ECS requires this server to do so (i.e transferring files to other server etc)
		ConnectionManager connection = new ConnectionManager(newTargetAddress, newTargetPort, KVServerInstance);
		connection.start(); 
	}
	

	private void tearDownConnection() throws IOException {
		logger.info("tearing down the connection to " + targetServerAddress + " : " + targetServerPort);
		if (clientSocket != null) {
			//input.close();
			//output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection " + targetServerAddress + " : " + targetServerPort + " closed");
			
		}
	}
	
	public void run() {
		try {

			clientSocket = new Socket(this.targetServerAddress, this.targetServerPort);

			setRunning(true);
			logger.info("Connection to " + targetServerAddress + " : " + targetServerPort +" established");


			
			output = clientSocket.getOutputStream();

			this.registerKVServer(); //register the server to the ecs


		
			while(isRunning()) {
				try {
					Message latestMsg = receiveMessage();
					handleNewMessage(latestMsg);
				} catch (IOException ioe) {
					if(isRunning()) {
						//logger.error("Connection lost from " + targetServerAddress + " : " + targetServerPort);
						disconnect();
						//System.out.println("I'm here right after disconnect has been called");
					}
				}
				//System.out.println("I'm here while running in run");				
			}
		} catch (IOException ioe) {
			logger.error("Connection from " + targetServerAddress + " : " + targetServerPort +  "could not be established!");
			
		} finally{
			
		}
	}



	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	

	public void sendMessage(Message msg) throws IOException {
		String messageResponse = msg.getStringMessage();
		byte[] msgBytes = msg.toByteArray(messageResponse);
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getStringMessage() + "'");
    }
	
	
	public Message receiveMessage() throws IOException {

		input = clientSocket.getInputStream();
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* carriage return */
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
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
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
		
		logger.info("Receive message:\t '" + msg.getStringMessage() + "'");
		return msg;
	}

	public Message convertStringToMessage(String requestString){
		
		KVMessage.StatusType status = null;
		String key = null;
		String value = null;
		
		if (requestString == ""){
			return null;
		}

		String[] msgRequest = requestString.trim().split(" ");
		String statusString = msgRequest[0].toUpperCase();

		status = KVMessage.StatusType.valueOf(statusString);

		if (msgRequest.length > 1){
			key = msgRequest[1];
		}

		if (msgRequest.length > 2){
			value = String.join(" ", Arrays.copyOfRange(msgRequest,2,msgRequest.length));
		}

		Message msg = new Message(key,value,status);

		return msg;
	}
	

	public void handleNewMessage(KVMessage msg) {


		KVMessage.StatusType msgType = msg.getStatus();

		switch (msgType){
			case METADATA_UPDATE:
				KVServerInstance.setMetaData (stringToMetadata(msg.getKey()));
				if (KVServerInstance.getMetaData().isEmpty()) {
					System.out.println("Metadata is empty. No ECS nodes are currently in the system.");
				} else {
					System.out.println("Current KVServer Metadata:");
					for (Map.Entry<BigInteger, ECSNode> entry : KVServerInstance.getMetaData().entrySet()) {
						ECSNode node = entry.getValue();
						node.printNodeDetails();
					}
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
					if (this.dataTransferTarget != ""){ 
						TreeMap<String, String> KVtransfers = this.KVServerInstance.getKVsToBeTransferred();
						Message target = new Message(this.dataTransferTarget, null, KVMessage.StatusType.DATA_TRANSFER_START);
						sendMessageSafe(target);
						if (KVtransfers.size() != 0){
							this.updateAllReplicas = true;
							sendMapToECS(KVtransfers);
						} else{
							logger.info("No data found to be transferred");
						}
						sendMessageSafe(new Message(null, null, KVMessage.StatusType.DATA_TRANSFER_COMPLETE));
					} 
					this.dataTransferTarget = "";

					//handle adding/deleting replicas based on metadata update
					int currNumServers = this.KVServerInstance.getMetaDataSize();
					if(currNumServers == 1){
						if (currNumServers < this.prevNumServers){ //case where num servers goes from 2->1
							if(this.KVServerInstance.disk.replicaExists(1)){
								this.KVServerInstance.disk.deleteReplica(1);
							}
						}
						this.updateAllReplicas = false;
					}
					if(currNumServers == 2){
						if (currNumServers < this.prevNumServers){ //case where num servers goes from 3->2
							if(this.KVServerInstance.disk.replicaExists(2)){
								this.KVServerInstance.disk.deleteReplica(2);
							}
						} else{
							if(!this.KVServerInstance.disk.replicaExists(1)){
								this.KVServerInstance.disk.createReplicaDirectory(1);
							}
						}
					}
					if(currNumServers == 3){
						if (currNumServers > this.prevNumServers){ //case where num servers goes from 2->3
							if(!this.KVServerInstance.disk.replicaExists(2)){
								this.KVServerInstance.disk.createReplicaDirectory(2);
							}
						}
					}
					this.prevNumServers = currNumServers;

					if(this.updateAllReplicas){
						sendReplicastoECS();
					}
					this.updateAllReplicas = false;
				}
				break;
			case INITIALIZE_DATA_TRANSFER:
				this.dataTransferTarget = msg.getKey();
				this.updateAllReplicas = true;
				break;
			case DATA_TRANSFER_START:
				//write lock (receiving data)
				this.KVServerInstance.updateWriteLock("lock");
				break;
			case DATA_TRANSFER:
				//handle data received from ecs and put into disk
				String key = msg.getKey();
				String value = msg.getValue();
				try{
					this.KVServerInstance.putKV(key, value);
				} catch(Exception e){
					logger.error("Failed to put key (Data transfer):" + key + " - " + e.getMessage());
				}
				break;
			case DATA_TRANSFER_COMPLETE:
				//disable write lock
				this.KVServerInstance.updateWriteLock("unlock");
				//call function to update all replicas
				sendReplicastoECS();
				break;
			case SHUTDOWN:
				try{
					TreeMap<String, String> allKVs = this.KVServerInstance.getAllKVs();
					if (allKVs.size() != 0){
						sendMessageSafe(new Message(null, null, KVMessage.StatusType.DATA_TRANSFER_START));
						sendMapToECS(allKVs);
						sendMessageSafe(new Message(null, null, KVMessage.StatusType.DATA_TRANSFER_COMPLETE));
						System.out.println("Data transfer to ECS complete");
					} else{
						System.out.println("No data transfer necessary");
					}
					//delete replicas once done
					this.KVServerInstance.disk.deleteReplicas();
					//delete disk once done 
					this.KVServerInstance.clearStorage();
					System.out.println("Deleting disk");
				} catch (Exception e){
					logger.error("Failed to get all KVs:" + e.toString());
				}
				this.shuttingDown = true;
				break;
			case SHUTDOWN_LAST: //if receive message from ECS and last server standing don't send data and don't delete disk
				System.out.println("Last server, saving disk for persistence");
				this.shuttingDown = true;
				break;
			case HEARTBEAT_PING:
				replyHeartbeat();
				//send KV update if needed
				if(this.KVServerInstance.newKVPut){
					if(this.prevNumServers > 1){
						sendNewKVstoReplicas();	
					} else {
						this.KVServerInstance.newKVKey.clear();
						this.KVServerInstance.newKVValue.clear();
						this.KVServerInstance.newKVPut = false;
					}			
				}
				break;
			case UPDATE_REPLICAS:
				this.updateAllReplicas = true;
				break;
			case INSERT_REPLICA_1:
				//insert replica 1 to disk
				logger.info("Inserting replica 1 into disk");
				this.KVServerInstance.disk.insertReplicaIntoDisk(1);
				this.KVServerInstance.disk.deleteReplica(1);
				this.updateAllReplicas = true;
				break;
			case INSERT_REPLICA_2:
				//insert replica 1 and 2 to disk
				this.KVServerInstance.disk.insertReplicaIntoDisk(1);
				this.KVServerInstance.disk.deleteReplica(1);
				this.KVServerInstance.disk.insertReplicaIntoDisk(2);
				this.KVServerInstance.disk.deleteReplica(2);
				this.updateAllReplicas = true;
				break;
			case NEW_REPLICA_1:
				//delete files in replica 1
				if(this.KVServerInstance.disk.replicaExists(1)){
					this.KVServerInstance.disk.deleteReplica(1);
				}
				this.KVServerInstance.disk.createReplicaDirectory(1);
				break;
			case NEW_REPLICA_2:
				//delete files in replica 2
				if(this.KVServerInstance.disk.replicaExists(2)){
					this.KVServerInstance.disk.deleteReplica(2);
				}
				this.KVServerInstance.disk.createReplicaDirectory(2);
				break;
			case REPLICA_1:
				//insert KV into replica 1
				String key1 = msg.getKey();
				String value1 = msg.getValue();
				try{
					this.KVServerInstance.disk.replicaAddKV(key1, value1, 1);
				} catch(Exception e){
					logger.error("Failed to put KV Pair (Replica 1):" + key1 + ":" + value1 + " - " + e.getMessage());
				}
				break;
			case REPLICA_2:
				//insert KV into replica 2
				String key2 = msg.getKey();
				String value2 = msg.getValue();
				try{
					this.KVServerInstance.disk.replicaAddKV(key2, value2, 2);
				} catch(Exception e){
					logger.error("Failed to put KV Pair (Replica 2):" + key2 + ":" + value2 + " - " + e.getMessage());
				}
				break;
		default:
				System.out.println("Error Handling message from ECS:" + msg.getStringMessage());
				break;
		}

	} 

	public void replyHeartbeat(){
		Message msg = new Message("", "", KVMessage.StatusType.HEARTBEAT_REPLY);
		sendMessageSafe(msg);
	}

	public void sendNewKVstoReplicas(){
		if(this.prevNumServers > 1){ 
			Message addr = new Message(this.KVServerInstance.getReplicaAddress(1), null, KVMessage.StatusType.REPLICA_1_DEST);
			sendMessageSafe(addr);
			for(int i = 0; i < this.KVServerInstance.newKVKey.size(); i++){
				Message kv = new Message(this.KVServerInstance.newKVKey.get(i), this.KVServerInstance.newKVValue.get(i), KVMessage.StatusType.REPLICA_1);
				sendMessageSafe(kv);
			}
		}
		if(this.prevNumServers > 2){
			Message addr = new Message(this.KVServerInstance.getReplicaAddress(2), null, KVMessage.StatusType.REPLICA_1_DEST);
			sendMessageSafe(addr);
			for(int i = 0; i < this.KVServerInstance.newKVKey.size(); i++){
				Message kv = new Message(this.KVServerInstance.newKVKey.get(i), this.KVServerInstance.newKVValue.get(i), KVMessage.StatusType.REPLICA_2);
				sendMessageSafe(kv);
			}
		}
		this.KVServerInstance.newKVKey.clear();
		this.KVServerInstance.newKVValue.clear();
		this.KVServerInstance.newKVPut = false;
	}

	public void sendMapToECS(TreeMap<String,String> KVs){
		for (Map.Entry<String, String> entry : KVs.entrySet()){
			String key = entry.getKey();
			String value = entry.getValue();
			Message kv = new Message(key, value, KVMessage.StatusType.DATA_TRANSFER);
			sendMessageSafe(kv);
		}
	}
	public void sendReplicastoECS(){
		if(this.prevNumServers > 1){
			try{
				Message addr = new Message(this.KVServerInstance.getReplicaAddress(1), null, KVMessage.StatusType.NEW_REPLICA_1);
				sendMessageSafe(addr);
				TreeMap<String, String> allKVs = this.KVServerInstance.getAllKVs();
				if (allKVs.size() != 0){
					for (Map.Entry<String, String> entry : allKVs.entrySet()){
						String key = entry.getKey();
						String value = entry.getValue();
						Message kv = new Message(key, value, KVMessage.StatusType.REPLICA_1);
						sendMessageSafe(kv);
					}
				}
				if(this.prevNumServers > 2){
					Message addr2 = new Message(this.KVServerInstance.getReplicaAddress(2), null, KVMessage.StatusType.NEW_REPLICA_2);
					sendMessageSafe(addr2);
					if (allKVs.size() != 0){
						for (Map.Entry<String, String> entry : allKVs.entrySet()){
							String key = entry.getKey();
							String value = entry.getValue();
							Message kv2 = new Message(key, value, KVMessage.StatusType.REPLICA_2);
							sendMessageSafe(kv2);
						}
					}
				}
			} catch (Exception e){
				logger.error("Failed to get all KVs:" + e.toString());
			}
		}
	}
	public void sendMessageSafe(Message msg){ 
		try{
			sendMessage(msg);
		}
		catch(Exception e){
			logger.error("Failed to send message : " + e.toString());
		}
	}

	public void registerKVServer(){
		try{
			logger.info("Registering server instance " + this.KVServerInstance.getHostname() + " : " + this.KVServerInstance.getPort());
			Message msg = new Message(this.KVServerInstance.getHostname(),Integer.toString(this.KVServerInstance.getPort()), KVMessage.StatusType.REGISTER_SERVER);
			sendMessage(msg);
		}
		catch(Exception e){
			logger.error("Failed to register server " + e);
		}
	}

	public static TreeMap<BigInteger, ECSNode> stringToMetadata(String metadataString) {
        TreeMap<BigInteger, ECSNode> metadata = new TreeMap<>();
        String[] nodes = metadataString.split(";");
        for (String node : nodes) {
            if (node.isEmpty()) continue;
            String[] parts = node.split(":");
            BigInteger key = new BigInteger(parts[0]);
            String[] nodeDetails = parts[1].split(",");
            String host = nodeDetails[0];
            int port = Integer.parseInt(nodeDetails[1]);
            String startingHashIdx = nodeDetails[2];
            String endingHashIdx = nodeDetails[3];
            ECSNode ecsNode = new ECSNode("Server " + host, host, port, startingHashIdx, endingHashIdx);
            metadata.put(key, ecsNode);
        }
        return metadata;
    }

	
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

			System.out.println("Connected to target server/ecs");

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.println("Connection terminated: " 
					+ targetServerAddress + " / " + targetServerPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ targetServerAddress + " / " + targetServerPort);
		}
		
	}

	public void shutdownServer(){
		Message msg = new Message("","",KVMessage.StatusType.SHUTDOWN);
		try{
			sendMessage(msg);
			while(this.shuttingDown == false){
				Thread.sleep(10);
				//wait for shutdown response from ecs
				//System.out.println("I'm here inside the send message while loop");
			}
			System.out.println("shutdown Hook complete");
		}
		catch(Exception e){
			System.out.println("Failed sending message to ECS" +e);
		}
	}


}

