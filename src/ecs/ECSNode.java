package ecs;

public class ECSNode implements IECSNode {

    private String name;
    private String host;
    private String startingHashIdx; 
    private String endingHashIdx; 
    private int port;

    public ECSNode() {}

    public ECSNode(String name, String host, int port, String startingHashIdx, String endingHashIdx) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.startingHashIdx = startingHashIdx; 
        this.endingHashIdx = endingHashIdx;
    }

    @Override
    public String getNodeName() {
        return name;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[] {this.startingHashIdx, this.endingHashIdx}; 
    }

    public String getStartingHashIdx() {
        return startingHashIdx;
    }

    public String getEndingHashIdx() {
        return endingHashIdx;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setStartingHashIdx(String startingHashIdx) {
        this.startingHashIdx = startingHashIdx; 
    }

    public void setEndingHashIdx(String endingHashIdx) {
        this.endingHashIdx = endingHashIdx;
    }

    /*public boolean contains(String hashValue) {
        if ((startingHashIdx.compareTo(endingHashIdx) >= 0) &&
                ((hashValue.compareTo(startingHashIdx) >= 0) ||
                (hashValue.compareTo(endingHashIdx) < 0))) {
            return true;
        } else if ((startingHashIdx.compareTo(endingHashIdx) <= 0) &&
                (hashValue.compareTo(startingHashIdx) >= 0) &&
                (hashValue.compareTo(endingHashIdx) < 0)) {
            return true;
        }
        return false;
    }*/

    public boolean contains(String hashValue){
        //case where we only have 1 node
        if(startingHashIdx.compareTo(endingHashIdx) == 0){
            return true;
        } else if ((hashValue.compareTo(startingHashIdx) > 0) && (hashValue.compareTo(endingHashIdx) <= 0)){ //case where key is in between starting and ending hash values
            return true;
        } else if (startingHashIdx.compareTo(endingHashIdx) > 0){
            if((hashValue.compareTo(startingHashIdx) > 0) || (hashValue.compareTo(endingHashIdx) <= 0)){ //case where server wraps around so contained if greater than starting value and smaller than ending value
                return true;
            }
        }
        return false;
    }


    public void printNodeDetails() {
        System.out.println("ECSNode Name: " + name + ", Host: " + host + ", Port: " + port);
        System.out.println("Starting Hash Index: " + startingHashIdx + ", Ending Hash Index: " + endingHashIdx);
    }
}