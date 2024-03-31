package app_kvClient;

import client.KVCommInterface;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVStore;
import client.KVCommInterface;
import shared.messages.KVMessage;
import shared.messages.Message;




public class KVClient implements IKVClient {
    
    public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private KVStore store = null;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	
	public void run() {
		// Create a new thread to run KVStore's run() method
		// Thread storeThread = new Thread(store);
		// storeThread.start();
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}

		// Wait for the store thread to finish
		// try {
		// 	storeThread.join();
		// } catch (InterruptedException e) {
		// 	logger.error("Interrupted while waiting for store thread to finish: " + e.getMessage());
		// }
	}

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
        store = new KVStore(hostname, port);

		store.addListener(this);

		store.connect(); 
    }

    private void disconnect() {
		if(store != null) {
			store.disconnect();
			store = null;
		}
	}

	public boolean getStop (){
		return this.stop;
	}

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return this.store;
    }



    public void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		// for (int i = 0; i < tokens.length; i++) {
        //     System.out.println(tokens[i]);
        // }
		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					newConnection(serverAddress, serverPort);
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (Exception e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}
		} else if (tokens[0].equals("put")) {
			if(tokens.length >= 2) {
				if(store != null){
					String key = tokens[1];
					StringBuilder value = new StringBuilder();
                    for (int i = 2; i < tokens.length; i++) {
                        value.append(tokens[i]);
                        if (i != tokens.length - 1) {
                            value.append(" ");
                        }
                    }
					try {
						KVMessage receivedMessage = store.put(key, value.toString());
						this.handleNewMessage(receivedMessage);
					} catch (Exception e){
						logger.error("Failed to send PUT message to server " + e.getMessage());
					}
					
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No message passed!");
			}
			
		} else if (tokens[0].equals("get")) {
			if(tokens.length == 2) {
				if(store != null){
					String key = tokens[1];
					try {
						KVMessage receivedMesage = store.get(key);
						this.handleNewMessage(receivedMesage);
					}
					catch(Exception e){
						logger.error("Failed to send GET message to server " + e.getMessage());
					}
				} else {
					printError("Not connected!");
				}
			} else {
				printError("Invalid number of parameters! Usage: put <key> <value>");
			}
			
		} else if (tokens[0].equals("keyrange")){

			if (store != null){
				try{
					KVMessage receivedMessage = store.requestMetadata(false);
					this.handleNewMessage(receivedMessage);
				}
				catch(Exception e){
					logger.error("Failed to send KEYRANGE message to server" + e.getMessage());
				}
			}
		} else if (tokens[0].equals("keyrange_read")){
			if (store != null){
				try{
					KVMessage receivedMessage = store.requestMetadata(true);
					this.handleNewMessage(receivedMessage);
				}
				catch(Exception e){
					logger.error("Failed to send KEYRANGE_READ message to server" + e.getMessage());
				}
			}
		}
		else if(tokens[0].equals("disconnect")) {
			disconnect();
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

    private void sendMessage(String msg){
		try {
			Message msgToSend = store.convertStringToMessage(msg);
			store.sendMessage(msgToSend);
		} catch (IOException e) {
			printError("Unable to send message!");
			disconnect();
		}
	}

    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t put <key> <value> pair to the server \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t get the <value> of <key> from the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

    private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	public void handleNewMessage(KVMessage msg) {
		
		if(!stop) {
			System.out.println(PROMPT + msg.getStringMessage());
		}
	}
	
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}
		
	}


    private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
	
    /**
     * Main entry point for the KVClient application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
