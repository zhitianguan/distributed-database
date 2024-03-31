package app_kvECS;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.math.BigInteger;
import java.security.*;

import ecs.IECSNode;
import ecs.ECSNode;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvECS.ClientConnection;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import shared.messages.KVMessage;
import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ECSClient implements IECSClient,Runnable {  

    private static Logger logger = Logger.getRootLogger();
    private String address;
    private int port;
    
    private InetAddress inetAddress;
    private ServerSocket serverSocket;
    private boolean running;
    private boolean shouldSendHeartbeat = true;
    private ECSNode dataTransferTarget;

    private static ECSClient instance;


    
    //ordered mapping: key is end Idx of server, value is ECSNode object
    private TreeMap<BigInteger, ECSNode> metadata = new TreeMap<>();

    private List<ClientConnection> clientConnections = new CopyOnWriteArrayList<>();
    private Socket client;
    private ScheduledExecutorService heartbeatScheduler;

    private Exception errorOccured = null;

    public enum ReplicaEventType{
        SERVER_DISCONNECTED,
        SERVER_SHUTDOWN,
        SERVER_ADDED
    }

    public void scheduleHeartbeat() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (shouldSendHeartbeat){ //we can change this variable if we don't want to send heartbeat for a certain event
                    try {
                        sendHeartbeat();
                    } catch (Exception e) {
                        logger.error("Error during heartbeat", e);
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }


    public ECSClient(String address,int port){
        this.address = address;
        this.port = port;
    }

    public int getPort(){
        return this.port;
    }

    public String getHostName(){
        return this.address;
    }

    public int getMetaDataSize(){
        return this.metadata.size();
    }

    public String getDataTransferTarget(){
        return this.dataTransferTarget.getNodeHost() + ":" + this.dataTransferTarget.getNodePort();
    }

    private boolean isRunning(){
        return this.running;
    }


    public static synchronized ECSClient getInstance (String address, int port) {
        if (instance == null) {
            instance = new ECSClient(address, port);
        }
        return instance;
    }


    public void run(){
        this.start();
    }

    @Override
    public boolean start() {
        running = initializeServer();
        scheduleHeartbeat();
        if (serverSocket != null){
            while(isRunning()){
                try{
                    this.client = serverSocket.accept();
                   
                    logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
                            +  " on port " + client.getPort());

                    
                    ClientConnection connection = new ClientConnection(client, ECSClient.getInstance(address, port));
                    this.addConnectionThread(connection); //instance is the object we want to always keep updating. This is the "shared" ecs object by all connections

                    
                    new Thread(connection).start();
                                        

                    //handle adding of nodes here
                    //e.g call addNode();  ,here update the metadata of instance and broadcast to all clients the new metadata

                    //this.addNode("default caching strat", 0);
                    
                }
                catch (IOException e) {
	            	logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                    this.errorOccured = e;
                            
	            }

            }
        }

        try{
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " + "Unable to close socket connection to server");
        }


        return false;
    }

    public Exception getError(){
        return this.errorOccured;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        running = false;
        if (heartbeatScheduler!=null && heartbeatScheduler.isShutdown() == false){
            heartbeatScheduler.shutdown();

        }
        return false;
    }

    private void addConnectionThread(ClientConnection connection){
        this.clientConnections.add(connection);
    }


    private boolean initializeServer() {
    	logger.info("Initializing ECS ...");
    	try {
			InetAddress addr = InetAddress.getByName(address);
            serverSocket = new ServerSocket(port, 0, addr);
            logger.info("ECS listening on port: " 
					+ serverSocket.getLocalPort());  
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open ECS socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    public void sendToAllClients(Message message) {

        for (ClientConnection clientConnection : clientConnections) {
            Socket socket = clientConnection.getClientSocket();
            String currentClientAddress = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
            logger.info("Sending message to server : " + currentClientAddress);
            clientConnection.sendMessageSafe(message);
        }

    }

    public void sendToClient(String clientFullAddress, Message message){

        for (ClientConnection clientConnection : clientConnections) {
            String currentClientAddress = clientConnection.getServerAddress() + ":" + clientConnection.getServerPort();
            if (currentClientAddress.equals(clientFullAddress)){
                logger.info("Sending message to server : " + currentClientAddress);
                clientConnection.sendMessageSafe(message);
                break; 
            }
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

    

    private String bigIntegerToString(BigInteger input){
        String output = input.toString(16);
        //need to zero pad it if necessary to get the full 32 chars
        while(output.length() < 32 ){
            output = "0"+output;
        }
        return output;
    }




@Override
public IECSNode addNode(String serverAddress, int serverPort,String cacheStrategy, int cacheSize) {
    
    String serverAddressPort = serverAddress + ":" + serverPort;

    //String serverAddressPort = client.getInetAddress().getHostAddress() + ":"+ client.getPort();
    BigInteger newServerEndingIdxBigInt = hash(serverAddressPort);
    String newServerEndingIdx = bigIntegerToString(hash(serverAddressPort));


    ECSNode newNode;
    if (metadata.size() == 0){
        // first server has startingIdx == endingIdx since it would cover the entire hash range
        newNode = new ECSNode("Server " + serverAddress,
            serverAddress, serverPort, newServerEndingIdx, newServerEndingIdx); 
        metadata.put(newServerEndingIdxBigInt, newNode);
        
    } else {
        Map.Entry<BigInteger, ECSNode> lowerEntry = metadata.lowerEntry(newServerEndingIdxBigInt);
        ECSNode predecessor;
        if (lowerEntry == null) {
            predecessor = metadata.lastEntry().getValue();
        } else {
            predecessor = lowerEntry.getValue();
        }

        String newServerStartingIdx = newServerEndingIdx;
        newServerStartingIdx = predecessor.getEndingHashIdx();

        newNode = new ECSNode("Server " + serverAddress,
            serverAddress, serverPort, newServerStartingIdx, newServerEndingIdx);
        metadata.put(newServerEndingIdxBigInt, newNode);

        // Finding and updating the successor
        Map.Entry<BigInteger, ECSNode> higherEntry = metadata.higherEntry(newServerEndingIdxBigInt);
        ECSNode successor;
        if (higherEntry == null) {
            successor = metadata.firstEntry().getValue();
        } else {
            successor = higherEntry.getValue();
        }
        if (successor != null) {
            successor.setStartingHashIdx(newServerEndingIdx);
        }
        String predecessorFullAddress = predecessor.getNodeHost() + ":" + predecessor.getNodePort();
        //String successorFullAddress = successor.getNodeHost() + ":" + successor.getNodePort();
        Message initServerTransfer = new Message(serverAddressPort,"",KVMessage.StatusType.INITIALIZE_DATA_TRANSFER);
        this.sendToClient(predecessorFullAddress, initServerTransfer);
    }


    


    Message msg = new Message(metadataToString(), null, KVMessage.StatusType.METADATA_UPDATE);
    this.sendToAllClients(msg);



    System.out.println("Successfully added server to hashring");
    printMetaData();
    return newNode; 
}


    public void handleReplicaLogic(String clientAddress,ReplicaEventType type){

        BigInteger hashedServerAddress = hash(clientAddress);

        switch (type){

            case SERVER_DISCONNECTED:
                logger.info("Handling server removal here" + clientAddress + " here");
                //BigInteger disconnectedServer = metadata.get(hashedServerAddress).getKey();
                Map.Entry<BigInteger, ECSNode> serverReplica = metadata.higherEntry(hashedServerAddress);
                if(serverReplica == null){
                    serverReplica = metadata.firstEntry();
                }
                ECSNode replicaNode = serverReplica.getValue();
                String replicaAddress = replicaNode.getNodeHost() + ":" + replicaNode.getNodePort();
                //send message to move replica1 into server's disk
                Message insertReplica1 = new Message(null, null, KVMessage.StatusType.INSERT_REPLICA_1);
                //need to find a way to track whether or not the client receives the message for case where two subsequent servers are dropped
                logger.info("Sending insert replica to address : " + replicaAddress);
                this.sendToClient(replicaAddress, insertReplica1);
                //if (message not sent){}
                
                //find two predecessor servers 
                if(this.metadata.size() > 2){ //case where 3 or more servers -> 2 or more servers
                    Map.Entry<BigInteger, ECSNode> predecessor1 = metadata.lowerEntry(hashedServerAddress);
                    if(predecessor1 == null){
                        predecessor1 = metadata.lastEntry();
                    }
                    ECSNode node1 = predecessor1.getValue();
                    String predecessor1Addr = node1.getNodeHost() + ":" + node1.getNodePort();
                    Message updateReps = new Message(null, null, KVMessage.StatusType.UPDATE_REPLICAS);
                    this.sendToClient(predecessor1Addr, updateReps);
                    if(this.metadata.size() > 3){ //case where 4 or more servers -> 3 or more servers
                        Map.Entry<BigInteger, ECSNode> predecessor2 = metadata.lowerEntry(predecessor1.getKey());
                        if(predecessor2 == null){
                            predecessor2 = metadata.lastEntry();
                        }
                        ECSNode node2 = predecessor2.getValue();
                        String predecessor2Addr = node2.getNodeHost() + ":" + node2.getNodePort();
                        this.sendToClient(predecessor2Addr, updateReps);
                    }
                }
                break;
            case SERVER_SHUTDOWN:
                if(this.metadata.size() > 2){ //case where 3 or more servers -> 2 or more servers
                    Map.Entry<BigInteger, ECSNode> predecessor1 = metadata.lowerEntry(hashedServerAddress);
                    if(predecessor1 == null){
                        predecessor1 = metadata.lastEntry();
                    }
                    ECSNode node1 = predecessor1.getValue();
                    String predecessor1Addr = node1.getNodeHost() + ":" + node1.getNodePort();
                    Message updateReps = new Message(null, null, KVMessage.StatusType.UPDATE_REPLICAS);
                    this.sendToClient(predecessor1Addr, updateReps);
                    if(this.metadata.size() > 3){ //case where 4 or more servers -> 3 or more servers
                        Map.Entry<BigInteger, ECSNode> predecessor2 = metadata.lowerEntry(predecessor1.getKey());
                        if(predecessor2 == null){
                            predecessor2 = metadata.lastEntry();
                        }
                        ECSNode node2 = predecessor2.getValue();
                        String predecessor2Addr = node2.getNodeHost() + ":" + node2.getNodePort();
                        this.sendToClient(predecessor2Addr, updateReps);
                    }
                }
                break;
            case SERVER_ADDED:
                logger.info("Handling added server here" + clientAddress + " here");
                if(this.metadata.size() >= 2){ //third or more server being added
                    //only need to cover the 2nd predecessor of the new server since the new server and precessor should be covered in data transfer
                    Map.Entry<BigInteger, ECSNode> predecessor1 = metadata.lowerEntry(hashedServerAddress);
                    if(predecessor1 == null){
                        predecessor1 = metadata.lastEntry();
                    }
                    Map.Entry<BigInteger, ECSNode> predecessor2 = metadata.lowerEntry(predecessor1.getKey());
                    if(predecessor2 == null){
                        predecessor2 = metadata.lastEntry();
                    }
                    ECSNode node2 = predecessor2.getValue();
                    String predecessor2Addr = node2.getNodeHost() + ":" + node2.getNodePort();
                    //could change this to UPDATE_REPLICA2 only or something if have time
                    Message updateReps = new Message(null, null, KVMessage.StatusType.UPDATE_REPLICAS);
                    this.sendToClient(predecessor2Addr, updateReps);
                }
                break;
            default:
                break;

        }

        //sendToClient(targetCoordinator, new Message('CoordinatorServer','',KVMessage.statusType.Coordinator));
        //sendToClient(targetReplica1, new Message('CoordinatorServer','',KVMessage.statusType.REPLICA_1));
        //sendToClient(targetReplica2,new Message('CoordinatorServer','',KVMessage.statusType.REPLICA_2));
    }

    public void sendHeartbeat(){
        Message message = new Message("", "", KVMessage.StatusType.HEARTBEAT_PING);
        for (ClientConnection clientConnection : clientConnections) {
            Socket socket = clientConnection.getClientSocket();
            //String currentClientAddress = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
            String currentClientAddress = clientConnection.getServerAddress() + ":" + clientConnection.getServerPort();
            logger.info("Sending message to server : " + currentClientAddress);
            try{
                clientConnection.sendMessage(message); //we want to catch error here, so don't use sendSafe 
            }
            catch (Exception e){
                logger.info("Failed sending message to server "+ currentClientAddress + ", server disconnected");
                handleReplicaLogic(currentClientAddress,ReplicaEventType.SERVER_DISCONNECTED);
                //removeNodes() call the regular removenodes logic here
                Collection<String> nodeNames = new ArrayList<>();
                nodeNames.add(currentClientAddress);
                removeNodes(nodeNames);
                this.clientConnections.remove(clientConnection);
                //send metadata update to all servers
                Message msg = new Message(metadataToString(), null, KVMessage.StatusType.METADATA_UPDATE);
                this.sendToAllClients(msg);
            }
        }
    }


    public void printMetaData() {
        if (metadata.isEmpty()) {
            System.out.println("Metadata is empty. No ECS nodes are currently in the system.");
        } else {
            System.out.println("Current ECS Metadata:");
            for (Map.Entry<BigInteger, ECSNode> entry : metadata.entrySet()) {
                ECSNode node = entry.getValue();
                node.printNodeDetails();
            }
            System.out.println("----------------------------------------------------------------------");
        }
    }



    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {

        try {

            for (String nodeName : nodeNames) {
                System.out.println(nodeName);

                if (metadata.size() == 0) {
                    break;
                }
                BigInteger hashKey = hash(nodeName);
                
                // Find the server to remove
                ECSNode serverToRemove = metadata.get(hashKey);
                if (serverToRemove == null) {
                    continue; // If the server is not found, skip to the next iteration
                }
                
                // Attempt to find the successor of the server to update
                Map.Entry<BigInteger, ECSNode> successorEntry = metadata.higherEntry(hashKey);
                if (successorEntry == null) {
                    // If no higher entry is found, wrap around to the lowest entry
                    successorEntry = metadata.firstEntry();
                }
                ECSNode successor = successorEntry.getValue();

                // Update the successor's starting hash index to that of the server to remove
                successor.setStartingHashIdx(serverToRemove.getStartingHashIdx());
                
                // Remove the server from metadata
                metadata.remove(hashKey);
                this.dataTransferTarget = successor;

                //update list of client connections here
                for(ClientConnection cc : this.clientConnections){
                    String connectionFullAddress = cc.getServerAddress() + ":" + cc.getServerPort();
                    //System.out.println("Checking nodeName:" + nodeName + "vs ccFullAddress:" + connectionFullAddress);
                    if(connectionFullAddress.equals(nodeName)){
                        clientConnections.remove(cc);
                        break;
                    }
                }

            }


            // Send metadata update message to all clients
            Message msg = new Message(metadataToString(), null, KVMessage.StatusType.METADATA_UPDATE);
            this.sendToAllClients(msg);



            System.out.println("Successfully removed server from hashring");
            printMetaData();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
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

    public static void main(String[] args) {
        //-a address -p port
        try{
            new LogSetup("logs/server.log", Level.ALL);
            int port = -1;
            String address = "127.0.0.1";
            for (int i = 0; i<args.length; i= i+2){
                switch (args[i]){
                    case "-a":
                        address = args[i+1];
                        break;
                    case "-p":
                        port = Integer.parseInt(args[i+1]);
                        break; 
                }
            }

            if (port == -1){
                logger.error("Please provide a port number: -p <port>");
            }
            else{
                ECSClient.getInstance(address, port).start();
            }            
         
        }
        catch (IOException e) {
        System.out.println("Error! Unable to initialize logger!");
        e.printStackTrace();
        System.exit(1);
        }
        catch(NumberFormatException nfe){
            System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
        }
    }
}
