package client;

import shared.messages.KVMessage;
import app_kvClient.KVClient;
import app_kvClient.KVClient.SocketStatus;

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
import app_kvServer.ConnectionManager;
import java.util.Map;
import java.util.TreeMap;
import java.math.BigInteger;
import java.security.*;


import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

import java.util.concurrent.CountDownLatch;

import ecs.IECSNode;
import ecs.ECSNode;

import shared.messages.KVMessage.StatusType;


public class KVStore extends Thread implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private Set<KVClient> listeners;
	private boolean running;
	
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	private String serverAddress;
	private int serverPort;


	private TreeMap<BigInteger, ECSNode> metadata = new TreeMap<>();
	

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.serverAddress = address;
		this.serverPort = port;
		listeners = new HashSet<KVClient>();
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		clientSocket = new Socket(this.serverAddress, this.serverPort);

		setRunning(true);
		logger.info("Connection established");

		this.start();

	}

	@Override
	public synchronized void disconnect() {
		logger.info("try to close connection ...");
		
		try {
			tearDownConnection();
			for(KVClient listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}
	

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			//input.close();
			//output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}


	@Override
	public KVMessage put(String key, String value) throws Exception {
		KVMessage message = null;
		try{
			sendMessage(new Message(key,value,KVMessage.StatusType.PUT));
			while (true){
			  try{
				message = receiveMessage();

				if (message == null){	

					boolean retry = handleServerDisconnected();	
					if (retry){
						sendMessage(new Message(key,value,KVMessage.StatusType.PUT));	

					}
					else{
						throw new Exception("Server is disconnected and metadata is empty");					
					}

				} else if (message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
					this.metadata = ConnectionManager.stringToMetadata(message.getKey());
					connectToNewServer(false, key);
					sendMessage(new Message(key,value,KVMessage.StatusType.PUT));

				} else {

					break;
					
				}
				}
				catch(Exception e){
					
					boolean shouldExit = handleDisconnectedIO(e.getMessage());
					if (shouldExit){
						throw new Exception("Server is disconnected and metadata is empty");		
					}
					//if not io exception, let it loop usually (based on testing) it hasn't finished reading
				}

			}
			return message;
		}
		catch(IOException e){
			if (handleDisconnectedIO(e.getMessage())){
				throw new Exception("Server is disconnected and metadata is empty");					
			}
			logger.error("Failed to send PUT message to server " + e);
			throw e;
		}

	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessage message = null;
		try{

			sendMessage(new Message(key,null,KVMessage.StatusType.GET));
			
			while (true){
			  try{
				message = receiveMessage();

				if (message == null){

					boolean retry = handleServerDisconnected();	
					if (retry){
						sendMessage(new Message(key,null,KVMessage.StatusType.GET));			
					}
					else{
						throw new Exception("Server is disconnected and metadata is empty");					
					}

				} else if (message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {

					this.metadata = ConnectionManager.stringToMetadata(message.getKey());
					connectToNewServer(false, key);
					sendMessage(new Message(key,null,KVMessage.StatusType.GET));
					
				} else {
					break;
				}
			  }
			  catch(Exception e){

				boolean shouldExit = handleDisconnectedIO(e.getMessage());
				if (shouldExit){
					throw new Exception("Server is disconnected and metadata is empty");					
				}
				//if not io exception, let it loop usually (based on testing) it hasn't finished reading
			   }
			}
			
		
			return message;
		}
		catch(Exception e){
			if (handleDisconnectedIO(e.getMessage())){
				throw new Exception("Server is disconnected and metadata is empty");					
			}
			logger.error("Failed to send GET message to server " + e.getMessage());
			throw e;
			}


	}

	public KVMessage requestMetadata(boolean keyrange_read) throws Exception{
		KVMessage message = null;
		try{
			if (!keyrange_read) {
				sendMessage(new Message(null,null,KVMessage.StatusType.KEYRANGE));
			} else {
				sendMessage(new Message(null,null,KVMessage.StatusType.KEYRANGE_READ));	
			}
				
			while (true){
				try{
				message = receiveMessage();

				if (message == null){
					boolean retry = handleServerDisconnected();	
					if (retry){
						if (!keyrange_read) {
							sendMessage(new Message(null,null,KVMessage.StatusType.KEYRANGE));	
						} else {
							sendMessage(new Message(null,null,KVMessage.StatusType.KEYRANGE_READ));	
						}
					}
					else{
						throw new Exception("Server is disconnected and metadata is empty");					
					}
				} 
				else {
					break;
				}
			}
			catch(Exception e){

				boolean shouldExit = handleDisconnectedIO(e.getMessage());
				if (shouldExit){
					throw new Exception("Server is disconnected and metadata is empty");				
				}
				//if not io exception, let it loop usually (based on testing) it hasn't finished reading
			}
			}
			
			if (!keyrange_read) {
				this.metadata = ConnectionManager.stringToMetadata(message.getKey());
				message = new Message(metadataToStringForKeyRange(), null, KVMessage.StatusType.KEYRANGE_SUCCESS);
			}
			return message;
		}
		catch(Exception e){
			if (handleDisconnectedIO(e.getMessage())){
				throw new Exception("Server is disconnected and metadata is empty");					
			}
		    logger.error("Failed to request metadata " + e.getMessage());
		    throw e;
		}

	}

	private boolean handleServerDisconnected() throws Exception {
		if (this.metadata.size() > 1) {
			logger.info("Server closed connection, connecting to a different server.");
			connectToNewServer(true, null);
			return true;
		} else {
			logger.error("Server is disconnected and metadata is empty.");
			return false;
		}
	}

	private boolean handleDisconnectedIO(String e) throws Exception {
		if (e.contains("Broken pipe") || e.contains("Write failed")) {
			logger.info("Server closed connection");
			return true;
		}
		else{
			return false;
		}
	}


public void connectToNewServer(boolean pickAnyServer, String key) {
    try {
        if (clientSocket != null && !clientSocket.isClosed()) {
            clientSocket.close(); // Close existing connection if open
        }

        ECSNode targetServer;
        if (pickAnyServer) {
			logger.info("Randomly connecting to a server");
            targetServer = selectAnyServer();
        } else {
            targetServer = selectServerForKey(key);
        }

        // Establish new connection
        this.serverAddress = targetServer.getNodeHost();
		this.serverPort = targetServer.getNodePort();
		logger.info("Connecting to " + this.serverAddress + ": " + this.serverPort);
        clientSocket = new Socket(serverAddress, serverPort);
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();

        logger.info("Connected to new server: " + serverAddress + ":" + serverPort);

        // Update any relevant client state here
        setRunning(true); // Mark as running if necessary
    } catch (Exception e) {
        logger.error("Failed to connect to new server: ", e);
    }
}

	private ECSNode selectAnyServer() {
		for (Map.Entry<BigInteger, ECSNode> entry : metadata.entrySet()) {
			ECSNode server = entry.getValue();
			// Check if the server is not the currently connected server
			if (!(server.getNodeHost().equals(this.serverAddress) && server.getNodePort() == this.serverPort)) {
				return server; // Return the first server that is not the currently connected one
			}
		}
		return null;
	}

	private ECSNode selectServerForKey(String key) {
		BigInteger keyHash = hash(key);
		Map.Entry<BigInteger, ECSNode> entry = metadata.ceilingEntry(keyHash);
		if (entry == null) {
			// If the key's hash is greater than the highest key, wrap around to the first server
			entry = metadata.firstEntry();
		}
		return entry.getValue();
	}


    private BigInteger hash(String valToHash) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(valToHash.getBytes());
            byte[] digest = m.digest();
            BigInteger bigInt = new BigInteger(1, digest);
            return bigInt;
        } catch (NoSuchAlgorithmException e) {
            // Handle the exception, e.g., log it or throw an unchecked exception
            throw new RuntimeException("Failed to get MD5 MessageDigest instance", e);
        }
    }


	
	
	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	public void addListener(KVClient listener){
		listeners.add(listener);
	}
	
	/**
	 * Method sends a Message using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(Message msg) throws IOException {
		output = clientSocket.getOutputStream();
		String messageResponse = msg.getStringMessage();
		byte[] msgBytes = msg.toByteArray(messageResponse);
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getStringMessage() + "'");
	}
	
	
	
	private Message receiveMessage() throws IOException {
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

			
			if (read == -1) {
				logger.info("Server closed the connection.");
				return null;
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

	public String metadataToStringForKeyRange() {
		
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<BigInteger, ECSNode> entry : metadata.entrySet()) {
            String key = entry.getValue().getNodeHost() + ":" + entry.getValue().getNodePort();
            ECSNode node = entry.getValue();
            sb.append(node.getStartingHashIdx());
            sb.append(",");
            sb.append(node.getEndingHashIdx());
            sb.append(",");
            sb.append(key.toString());
            sb.append(";");
        }
        return sb.toString();
    }

	
	

}
