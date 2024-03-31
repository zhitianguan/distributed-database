package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		FAILED, /*Unknown Command*/
		INVALID_PARAMETER, /*Request parameter is invalid*/
		SHUTDOWN, /*Server shutting down*/
		SHUTDOWN_LAST,
		SERVER_NOT_RESPONSIBLE,
		SERVER_WRITE_LOCK,
		SERVER_STOPPED,
		KEYRANGE_SUCCESS,
		TESTING_SEND,
		REGISTER_SERVER,
		METADATA_UPDATE,
		INITIALIZE_DATA_TRANSFER,
		DATA_TRANSFER,
		DATA_TRANSFER_START,
		DATA_TRANSFER_COMPLETE,
		KEYRANGE,
		KEYRANGE_READ,
		KEYRANGE_READ_SUCCESS,
		HEARTBEAT_PING,
		HEARTBEAT_REPLY,
		INSERT_REPLICA_1,
		INSERT_REPLICA_2,
		UPDATE_REPLICAS,
		NEW_REPLICA_1,
		NEW_REPLICA_2,
		REPLICA_1_DEST,
		REPLICA_2_DEST,
		REPLICA_1,
		REPLICA_2
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	public String getStringMessage();

	public byte[] toByteArray(String s);
	
}


