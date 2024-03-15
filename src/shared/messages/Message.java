package shared.messages;

public class Message implements KVMessage{
    
    private String key;
    private String value;
    private StatusType status;
    

    public Message(String key,String value,StatusType status){
        this.key = key;
        this.value = value;
        this. status = status;
    }

    public String getKey(){
        return this.key;
    }

    public String getValue(){
        return this.value;
    }

    public StatusType getStatus(){
        return this.status;
    }

    public String getStringMessage(){
        return this.status + " " +  this.key + " " + this.value;
    }

    /*public String getJsonString(){
        String jsonString = "{" + \"key\": \"" + this.key + "\"," +  "\"value\": \"" + this.value + "\"," + "\"status\": \"" + this.status + "\"" +
        "}";
        return jsonString;
    }*/

    public byte[] toByteArray(String s){  

        final char LINE_FEED = 0x0A;
	    final char RETURN = 0x0D;
        byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;	
    }
}
