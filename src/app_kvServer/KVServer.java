package app_kvServer;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import KVCache.KVCache;
import KVDisk.KVDisk;

import app_kvServer.ConnectionManager;

import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.math.BigInteger;
import java.security.*;

import ecs.IECSNode;
import ecs.ECSNode;


import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;




public class KVServer implements IKVServer, Runnable {
	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};

	private static Logger logger = Logger.getRootLogger();
	
	private String address;
	private int port;
	private String dataDirectory;
	private int cacheSize;
	private String strategy;
	private InetAddress inetAddress;
    private ServerSocket serverSocket;
	private boolean running;
	
	private static KVServer instance;
	private KVCache cache;
	public KVDisk disk;
	private int writeLock;

	private String ecsAddress;
	private int ecsPort;

	public boolean newKVPut;
	public ArrayList<String> newKVKey;
	public ArrayList<String> newKVValue;
	


	//ordered mapping: key is end Idx of server, value is ECSNode object
    private TreeMap<BigInteger, ECSNode> metadata = new TreeMap<>();


	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(String address, int port, String directory, int cacheSize, String strategy, String ecsAddress, int ecsPort) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		this.dataDirectory = directory;
		this.cacheSize = cacheSize; 
		this.strategy = strategy;
		this.cache = new KVCache(cacheSize /*,strategy*/);
		this.disk = new KVDisk(dataDirectory);
		this.writeLock = 0;
		this.ecsAddress = ecsAddress;
		this.ecsPort = ecsPort;
		this.newKVKey = new ArrayList<String>();
		this.newKVValue = new ArrayList<String>();
	}


	public KVServer(int port, int cacheSize, String strategy) {
		this("127.0.0.1", port, "", cacheSize, strategy,"",0); // Default address is localhost
	}
	

	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return this.port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return this.address;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return this.disk.diskKVExists(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return 	this.cache.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		try{
			if(inCache(key)){ //cache hit
				return this.cache.getCache(key);
			} else{ //cache miss get from disk and place in cache
				String value = this.disk.diskGetKV(key);
				this.cache.putCache(key, value);
				return value;
			}
		} catch(Exception e){
			return null;
		}
	}


	/*@Override
public KVMessage getKV(String key) throws Exception {
    // First, check if the key is in the server's range
    if (!isInRange(key)) {
        // If not in range, create a response indicating the server is not responsible
        // and include the most recent metadata.
        return new KVMessage(metadataToString(), null, KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
    }

    try{
		if(inCache(key)){ //cache hit
			return this.cache.getCache(key);
		} else{ //cache miss get from disk and place in cache
			String value = this.disk.diskGetKV(key);
			this.cache.putCache(key, value);
			return value;
		}
	} catch(Exception e){
		return null;
	}
}*/

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		try{
			this.disk.diskPutKV(key,value);
			this.cache.putCache(key,value);
		} catch (Exception e){
			//e.printStackTrace();
		}
	}

	public boolean isConnectedToECS(){
		return this.ecsAddress!= "" || this.ecsPort != 0;
	}

	public int getWriteLock(){
		return this.writeLock;
	}

	public void updateWriteLock(String str){
		switch(str){
			case "lock":
				this.writeLock += 1;
				break;
			case "unlock":
				this.writeLock -= 1;
				break;
			default:
				System.out.println("Error: Unknown write lock input");
				break;
		}
	}

	public void deleteKV(String key) throws Exception{
		try{
			if (inStorage(key)){
				if (inCache(key)){
					this.cache.removeCache(key);
				}
				this.disk.diskRemoveKV(key);
			}
		} catch(Exception e){
			//e.printStackTrace();
		}
	}

	public TreeMap<String,String> getAllKVs() throws Exception{
		return this.disk.getAllKV();
	}

	public TreeMap<String,String> getKVsToBeTransferred(){
		TreeMap<String, String> KVsToBeTransferred = new TreeMap<>();
		TreeMap<String, String> allKVs = this.disk.getAllKV();
		//ECSNode serverNode = getServerNode();

		for (Map.Entry<String, String> entry : allKVs.entrySet()){
			String key = entry.getKey();
			//if not in server range transfer
			if(!isInRange(key)){
				String value = entry.getValue();
				KVsToBeTransferred.put(key,value);
				try{
					this.deleteKV(key);
				} catch (Exception e){
					logger.error("Failed to delete KV");
				}
			}
		}
		return KVsToBeTransferred;
	}

	/*public ECSNode getServerNode(){
		String portStr = String.valueOf(this.port);
		String serverFullAddress = this.address + ":" + portStr;
		BigInteger hashedServerAddress = hash(serverFullAddress);
		return this.metadata.get(hashedServerAddress);
	}*/

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
		this.cache.clearCache();
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		this.disk.deleteDisk();
	}

	public String metadataToString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<BigInteger, ECSNode> entry : metadata.entrySet()) {
            BigInteger key = entry.getKey();
            ECSNode node = entry.getValue();
            sb.append(key.toString());
            sb.append(":");
            sb.append(node.getNodeHost());
            sb.append(",");
            sb.append(node.getNodePort());
            sb.append(",");
            sb.append(node.getStartingHashIdx());
            sb.append(",");
            sb.append(node.getEndingHashIdx());
            sb.append(";");
        }
        return sb.toString();
    }

	/**
     * Initializes and starts the server. 
     * Loops until the the server should be closed.
     */
	@Override
    public void run(){
		// TODO Auto-generated method stub
		running = initializeServer();
		if (this.ecsAddress != "" && isRunning()){
			ConnectionManager connection = new ConnectionManager(ecsAddress,ecsPort, instance);
			new Thread(connection).start();
		}
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
					Socket client = serverSocket.accept();  
	                ClientConnection connection = 
	                		new ClientConnection(client,instance);
	                new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			System.out.println("INSIDE CATCH PART");
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	public boolean isRunning() {
        return this.running;
	}

	public void setMetaData(TreeMap<BigInteger, ECSNode> metadata){
		this.metadata= metadata;
	}

	public TreeMap<BigInteger, ECSNode> getMetaData(){
		return this.metadata;
	}

	public String metadataToStringForKeyRange(boolean keyrange_read) {		
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<BigInteger, ECSNode> entry : this.metadata.entrySet()) {
            ECSNode node = entry.getValue();
            String key = node.getNodeHost() + ":" + node.getNodePort();
            sb.append(node.getStartingHashIdx());
            sb.append(",");
            sb.append(node.getEndingHashIdx());
            sb.append(",");
            sb.append(key);

			if (keyrange_read) {
				sb.append(",");

				// Finding and updating the 1st successor
				Map.Entry<BigInteger, ECSNode> higherEntry = this.metadata.higherEntry(new BigInteger(node.getEndingHashIdx(), 16));

				ECSNode successor1;
				if (higherEntry == null) {
					successor1 = this.metadata.firstEntry().getValue();
				} else {
					successor1 = higherEntry.getValue();
				}
				String key1 = successor1.getNodeHost() + ":" + successor1.getNodePort();


				sb.append(key1);
				sb.append(",");




				// Finding and updating the 2nd successor
				Map.Entry<BigInteger, ECSNode> higherEntry2 = this.metadata.higherEntry(new BigInteger(successor1.getEndingHashIdx(), 16));

				
				
				ECSNode successor2;
				if (higherEntry2 == null) {
					successor2 = this.metadata.firstEntry().getValue();
				} else {
					successor2 = higherEntry2.getValue();
				}


				String key2 = successor2.getNodeHost() + ":" + successor2.getNodePort();

				sb.append(key2);
        	}	

            sb.append(";");
        }
        return sb.toString();
    }
	
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print("Connection to ECS terminated");
			//System.out.print(PROMPT);
			//System.out.println("Connection terminated: " 
			//		+ serverAddress + " / " + serverPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.print("Connection to ECS lost");
			//System.out.println("Connection lost: " 
			//		+ serverAddress + " / " + serverPort);
			//System.out.print(PROMPT);
		}
		
	}

	private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {

			//check if instance has been initialized before
			if (instance == null){
				instance = this;
			}

			InetAddress addr = InetAddress.getByName(address);
            serverSocket = new ServerSocket(port, 0, addr);
            logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());  
			logger.info("With cache size: " + cacheSize);
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
			this.running = false;
			return false;
        }
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


	public boolean isInRange(String key) {
		//BigInteger keyHash = hash(this.address + ":" + this.port);
		Map.Entry<BigInteger, ECSNode> currentServerEntry = null;

		// Find the current server's metadata entry
		for (Map.Entry<BigInteger, ECSNode> entry : metadata.entrySet()) {
			ECSNode node = entry.getValue();
			if (node.getNodeHost().equals(this.address) && node.getNodePort() == this.port) {
				currentServerEntry = entry;
				break;
			}
		}

		if (currentServerEntry != null) {
			ECSNode currentServerNode = currentServerEntry.getValue();
			BigInteger hashedKey = hash(key);
			String strHashedKey = bigIntegerToString(hashedKey);
			return currentServerNode.contains(strHashedKey);
			/*BigInteger startHash = new BigInteger(currentServerNode.getStartingHashIdx(), 16);
			BigInteger endHash = new BigInteger(currentServerNode.getEndingHashIdx(), 16);

			if (startHash.compareTo(endHash) < 0) {
				// Normal case: startHash < endHash
				return keyHash.compareTo(startHash) > 0 && keyHash.compareTo(endHash) <= 0;
			} else {
				// Wrap-around case: startHash > endHash
				return keyHash.compareTo(startHash) > 0 || keyHash.compareTo(endHash) <= 0;
			}*/
		}

		return false;
	}
	public int getMetaDataSize(){
		return this.metadata.size();
	}

	//helper functions which find the server's replica1/2 address, returns the full address as a string or null if there is no replica
	public String getReplicaAddress(int replicaNum){
		String replicaAddress = null;
		if(getMetaDataSize() >= 2){
			for (Map.Entry<BigInteger, ECSNode> entry : metadata.entrySet()) {
				ECSNode node = entry.getValue();
				if (node.getNodeHost().equals(this.address) && node.getNodePort() == this.port) {
					Map.Entry<BigInteger, ECSNode> replica = metadata.higherEntry(entry.getKey());
					if(replica == null){
						replica = metadata.firstEntry();
					}
					if(replicaNum == 1){
						ECSNode replicaNode = replica.getValue();
						replicaAddress = replicaNode.getNodeHost() + ":" + replicaNode.getNodePort();
						return replicaAddress;
					} else if (getMetaDataSize() >= 3 && replicaNum == 2){
						replica = metadata.higherEntry(replica.getKey());
						if(replica == null){
							replica = metadata.firstEntry();
						}
						ECSNode replicaNode = replica.getValue();
						replicaAddress = replicaNode.getNodeHost() + ":" + replicaNode.getNodePort();
						return replicaAddress;
					}
				}
			}
		}
		return replicaAddress;
	}

	private String bigIntegerToString(BigInteger input){
        String output = input.toString(16);
        //need to zero pad it if necessary to get the full 32 chars
        while(output.length() < 32 ){
            output = "0"+output;
        }
        return output;
    }



	/**
     * helper function to setup the log  
     * @param logDirectory is the relative path of the logfile
	 * @param logLevel is the logLevel for LogSetup
     */
	public static void initLog(String logDirectory, String logLevel) {
		String logFileDirectory = "server.log";
		if (logDirectory != ""){
			logFileDirectory = logDirectory.concat("/server.log");
		} 
		try{
			switch (logLevel){
				case "ALL":
					new LogSetup(logFileDirectory, Level.ALL);
					break;
				case "DEBUG":
					new LogSetup(logFileDirectory, Level.DEBUG);
					break;
				case "INFO":
					new LogSetup(logFileDirectory, Level.INFO);
					break;
				case "WARN":
					new LogSetup(logFileDirectory, Level.WARN);
					break;	
				case "ERROR":
					new LogSetup(logFileDirectory, Level.ERROR);
					break;
				case "FATAL":
					new LogSetup(logFileDirectory, Level.FATAL);
					break;
				case "OFF":
					new LogSetup(logFileDirectory, Level.OFF);
					break;
				default:
					new LogSetup(logFileDirectory, Level.ALL);
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			int port = -1, ecsPort = -1, cacheSize = 0;
			String address = "127.0.0.1", dataDirectory = "", logDirectory = "", logLevel = "ALL" , ecsAddress = "";
			for (int i = 0; i < args.length; i = i + 2){
				switch (args[i]){
					case "-p":
						port = Integer.parseInt(args[i+1]);
						break;
					case "-a":
						address = args[i+1];
						break;
					case "-d":
						dataDirectory = args[i+1];
						break;
					case "-l":
						logDirectory = args[i+1];
						break;
					case "-ll":
						logLevel = args[i+1];
						break;
					case "-c":
						cacheSize = Integer.parseInt(args[i+1]);
						break;
					case "-h":
						System.out.println("M1 SERVER HELP (USAGE): java -jar m1-server.jar -p <port> -a <address> -d <dataDirectory> -l <logDirectory> -ll <logLevel>");
						System.exit(0); 
						break;
					case "-b":
						String[] fullAddress = args[i+1].split(":");
						ecsAddress = fullAddress[0];
						ecsPort = Integer.parseInt(fullAddress[1]);
						break;
				}
			}
			initLog(logDirectory, logLevel);


		

			if (port == -1){
				logger.error("Please provide a port number: -p <port>");
			}
			else{
				instance = new KVServer(address, port, dataDirectory, cacheSize, "cache strat", ecsAddress, ecsPort);
				/*if (ecsAddress != ""){
					ConnectionManager connection = new ConnectionManager(ecsAddress,ecsPort, instance);
					new Thread(connection).start();
				}*/
				instance.run();
				
			}

			

		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
    }
}

