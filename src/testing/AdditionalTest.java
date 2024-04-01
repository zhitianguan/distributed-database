package testing;


import client.KVStore;

import jdk.jfr.Timestamp;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import app_kvClient.KVClient;

import org.junit.Test;

import junit.framework.TestCase;
import app_kvServer.KVServer;


import app_kvServer.ConnectionManager;

import shared.messages.KVMessage;
import shared.messages.Message;

import java.util.TreeMap;
import java.math.BigInteger;

import ecs.IECSNode;
import ecs.ECSNode;
import app_kvECS.ECSClient;



public class AdditionalTest extends TestCase {

	
	// TODO add your test cases, at least 3
	@Test
    public void testInvalidNumberOfArgs() {
        String invalidNumArgsCommand = "connect ";
		Exception exception = null;
		try {
			KVClient kvClient = new KVClient();
            kvClient.handleCommand(invalidNumArgsCommand);
		}
		catch (Exception e) {
			exception = e;
		}

        assertNull(exception);
    }


	@Test
    public void testInvalidPortNum() {
        String invalidPortCommand = "connect 127.0.0.1 111111111111111111";
		Exception exception = null;
		try {
			KVClient kvClient = new KVClient();
            kvClient.handleCommand(invalidPortCommand);
		}
		catch (Exception e) {
			exception = e;
		}

        assertNull(exception);
    }



	@Test
    public void testInvalidHost() {
        String invalidHostCommand = "connect abcdef 1050";
		Exception exception = null;
		try {
			KVClient kvClient = new KVClient();
            kvClient.handleCommand(invalidHostCommand);
		}
		catch (Exception e) {
			exception = e;
		}

        assertNull(exception);
    }

	@Test
    public void testInvalidAction() {
        String invalidActionCommand = "hi localhost 100";
		Exception exception = null;
        try {
			KVClient kvClient = new KVClient();
            kvClient.handleCommand(invalidActionCommand);
		}
		catch (Exception e) {
			exception = e;
		}

        assertNull(exception);
	}	

	@Test
	public void testPersistence() {
		KVServer kvServer = new KVServer(6021, 100, "");
		new Thread(kvServer).start();

		String key = "school";
		String value = "UofT";
		String storedValue = null;
		KVMessage message = null;
		Exception ex = null;

		try {
			kvServer.putKV(key, value);
			storedValue = kvServer.getKV(key);
			kvServer.close();

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && storedValue == value);

	
		storedValue = null;
		System.gc();


		KVServer kvServerNew = new KVServer(7011, 100, ""); //new server in different port
		new Thread(kvServerNew).start();


		try {
			storedValue = kvServerNew.getKV(key);
			kvServerNew.close();

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && storedValue.equals(value));

	}
	

	
	@Test
	public void multipleConnect(){ 
		KVServer kvServer = new KVServer(8000, 100, "");
		new Thread(kvServer).start();
		Exception ex = null;

		KVStore client1 = new KVStore("localhost", 8000);
		KVStore client2 = new KVStore("localhost", 8000);

		try {
			client1.connect();
		} catch (Exception e) {
			ex = e;

		}

		assertTrue(ex== null);

		try{
			client2.connect();
			//kvServer.close();
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex == null);

	}

	//M2 ECS TESTS

	@Test
    public void testConnectToECS() {
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 1997;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);
		KVServer serverInstance = new KVServer("127.0.0.1",1950,".",100,"default",ecsAddress,ecsPort);
		TreeMap<BigInteger, ECSNode> initialMetadata = serverInstance.getMetaData();

		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(serverInstance).start();
			Thread.sleep(2000);
		}
		catch(Exception e){
			ex = e;
		}

		//assertTrue(serverInstance.getMetaData().size() == 1); //1 server is added at this point
		assertTrue(ex == null);
		serverInstance.close();
		ecsServer.stop();
	}

		
	@Test
	public void testConnectMultipleServersToECS(){ //equivalent to adding a node (server)
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 1998;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);
	

		KVServer kvServerOne = new KVServer("127.0.0.1",9114, "." , 100, "default",ecsAddress, ecsPort);
		KVServer kvServerTwo = new KVServer("127.0.0.1",9023,".",100,"default",ecsAddress, ecsPort);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			new Thread(kvServerTwo).start();
			Thread.sleep(3000);

		}
		catch(Exception e){
			ex = e;
		}

		//assertTrue(kvServerOne.metadataToString().equals(kvServerTwo.metadataToString()));
		assertTrue(ex == null);
		kvServerOne.close();
		kvServerTwo.close();
		ecsServer.stop();
	}

	@Test
    public void disconnectFromECS() { //equivalent to removing a node (server)
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 1999;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);
	

		KVServer kvServerOne = new KVServer("127.0.0.1",9023, ".", 100,"default", ecsAddress, ecsPort);
		KVServer kvServerTwo = new KVServer("127.0.0.1",9114,"." , 100,"default", ecsAddress,ecsPort);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			new Thread(kvServerTwo).start();
			Thread.sleep(1000);

		}
		catch(Exception e){
			ex = e;
		}

		//assertTrue(kvServerOne.metadataToString().equals(kvServerTwo.metadataToString()));
		//assertTrue(kvServerOne.getMetaData().size() == 2); 
		
		assertTrue(ex == null);

		try{
			kvServerOne.close(); //disconnecting one of the servers
			Thread.sleep(1000);
		}
		catch(Exception e){
			ex = e; 
		}

		//assertTrue(kvServerTwo.getMetaData().size() == 1); //updated metadata of server 2 should now only be 1 left (itself)
		assertTrue(ex == null);

		kvServerOne.close();
		kvServerTwo.close();
		ecsServer.stop();
	}


	@Test 
	public void getKeyrange(){
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 2001;
		KVMessage message = null;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);

		KVServer kvServerOne = new KVServer("127.0.0.1",9005, ".", 100,"default", ecsAddress, ecsPort);
		KVStore kvClient = new KVStore("127.0.0.1", 9005);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			Thread.sleep(1000);
			kvClient.connect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue (ex == null);

		try{
			message = kvClient.requestMetadata(false);
		}
		catch(Exception e){
			ex = e;
		}


		assertTrue (ex == null);

		assertTrue(message.getStatus() == KVMessage.StatusType.KEYRANGE_SUCCESS);

		kvServerOne.close();
		ecsServer.stop();
	}

	@Test 
	public void puttingToOutOfRange(){
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 2011;
		KVMessage message = null;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);

		KVServer kvServerOne = new KVServer("127.0.0.1",9021, "dir1", 100,"default", ecsAddress, ecsPort);
		KVServer kvServerTwo = new KVServer("127.0.0.1",9022,"dir2" , 100,"default", ecsAddress,ecsPort);
		KVStore kvClient = new KVStore("127.0.0.1", 9021);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			Thread.sleep(1000);
			new Thread(kvServerTwo).start();
			kvClient.connect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue (ex == null);

		try{
			message = kvClient.put("127.0.0.1:9022","this should be in server two though we are connected to server one");
		}
		catch(Exception e){
			ex = e;
		}


		assertTrue (ex == null);

		assertTrue(message.getStatus() == KVMessage.StatusType.PUT_SUCCESS || message.getStatus() == KVMessage.StatusType.PUT_UPDATE);  //client should recconect to correct server to put 

		kvServerOne.close();
		kvServerTwo.close();
		ecsServer.stop();

	}

	@Test 
	public void GettingOutOfRange(){
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 2011;
		KVMessage message = null;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);

		KVServer kvServerOne = new KVServer("127.0.0.1",9021, "dir1", 100,"default", ecsAddress, ecsPort);
		KVServer kvServerTwo = new KVServer("127.0.0.1",9022,"dir2" , 100,"default", ecsAddress,ecsPort);
		KVStore kvClient = new KVStore("127.0.0.1", 9021);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			Thread.sleep(1000);
			new Thread(kvServerTwo).start();
			kvClient.connect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue (ex == null);

		try{
			message = kvClient.get("127.0.0.1:9022");
		}
		catch(Exception e){
			ex = e;
		}


		assertTrue (ex == null);

		assertTrue(message.getStatus() == KVMessage.StatusType.GET_SUCCESS); //client should recconect to correct server to get 

		kvServerOne.close();
		kvServerTwo.close();
		ecsServer.stop();

	}

	@Test
	public void serverFailureTesting(){ //ensure ecs still functions when server disconnected (heartbeat handled properly)
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 2011;
		KVMessage message = null;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);

		KVServer kvServerOne = new KVServer("127.0.0.1",9021, "dir1", 100,"default", ecsAddress, ecsPort);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			Thread.sleep(1000);
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue (ex == null);

		try{
			kvServerOne.close();
		}
		catch(Exception e){
			ex = e;
		}


		assertTrue (ex == null);
		assertTrue (ecsServer.getError() == null);

		ecsServer.stop();

	}

	@Test 
	public void getKeyrangeRead(){
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 2001;
		KVMessage message = null;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);

		KVServer kvServerOne = new KVServer("127.0.0.1",9005, ".", 100,"default", ecsAddress, ecsPort);
		KVStore kvClient = new KVStore("127.0.0.1", 9005);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			Thread.sleep(1000);
			kvClient.connect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue (ex == null);

		try{
			message = kvClient.requestMetadata(true);
		}
		catch(Exception e){
			ex = e;
		}


		assertTrue (ex == null);

		assertTrue(message.getStatus() == KVMessage.StatusType.KEYRANGE_READ_SUCCESS);

		kvServerOne.close();
		ecsServer.stop();
	}


	@Test 
	public void GettingFromReplica(){ //server dies, get from replica server
		Exception ex = null;
		String ecsAddress = "127.8.8.8";
		int ecsPort = 2011;
		KVMessage message = null;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);

		KVServer kvServerOne = new KVServer("127.0.0.2",9021, "dir1", 100,"default", ecsAddress, ecsPort);
		KVServer kvServerTwo = new KVServer("127.0.0.2",9022,"dir2" , 100,"default", ecsAddress,ecsPort);
		KVStore kvClient = new KVStore("127.0.0.2", 9021);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			Thread.sleep(1000);
			new Thread(kvServerTwo).start();
			kvClient.connect();
		}
		catch(Exception e){
			ex = e;
		}

		assertTrue (ex == null);



		try{
			kvServerOne.close();
			message = kvClient.get("127.0.0.2:9022"); //client connected to kvservertwo
		}
		catch(Exception e){
			ex = e;
		}


		assertTrue (ex == null);

		assertTrue(message.getStatus() == KVMessage.StatusType.GET_SUCCESS); 

		kvServerTwo.close();
		ecsServer.stop();

	}

	@Test
	public void multipleServersKilled(){
		Exception ex = null;
		String ecsAddress = "127.7.7.7";
		int ecsPort = 2011;
		KVMessage message = null;

		ECSClient ecsServer = new ECSClient(ecsAddress,ecsPort);

		KVServer kvServerOne = new KVServer("127.0.0.3",9023, "dir1", 100,"default", ecsAddress, ecsPort);
		KVServer kvServerTwo = new KVServer("127.0.0.3",9024, "dir2", 100, "default", ecsAddress, ecsPort);


		try{
			new Thread(ecsServer).start();
			Thread.sleep(1000);
			new Thread(kvServerOne).start();
			Thread.sleep(1000);
			new Thread (kvServerTwo).start();
			Thread.sleep(1000);

		}
		catch(Exception e){
			ex = e;
		}

		assertTrue (ex == null);

		try{
			kvServerOne.close();
			kvServerTwo.close();
		}
		catch(Exception e){
			ex = e;
		}


		assertTrue (ex == null);
		assertTrue (ecsServer.getError() == null);

		ecsServer.stop();

	}

}

