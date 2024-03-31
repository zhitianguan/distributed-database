package KVDisk; 

import java.io.IOException;
import java.security.SignatureException;
import java.nio.file.Files;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVDisk {
    private String KVDiskPath;
    private String KVReplicaOnePath;
    private String KVReplicaTwoPath;
    private List<String> keysList;


    public KVDisk(String dataDirectory){
        boolean needLoadDisk = true;
        if (dataDirectory != ""){
            File directory = new File(dataDirectory);
            if(!directory.exists()){
                directory.mkdirs();
                needLoadDisk = false;
            }
            this.KVDiskPath = dataDirectory.concat("/");
        } else{
            this.KVDiskPath = ".";
        }

        this.keysList = new CopyOnWriteArrayList<String>();
        if(needLoadDisk){
            loadDisk();
        }
        //intialize path variables to null for boolean helper functions
        this.KVReplicaOnePath = null;
        this.KVReplicaTwoPath = null;
    }
    public void loadDisk() {
        System.out.println("Loading disk...");
        File diskDirectory = new File(KVDiskPath);
        File[] files = diskDirectory.listFiles(new FilenameFilter(){
            @Override
            public boolean accept(File dir, String name){
                return name.endsWith(".txt");
            }
        });

        if (files != null){
            System.out.println("Found files in disk...");
            for (File file : files){
                String fileName = file.getName();
                //remove ".txt" extension from name before adding to keysList
                String key = fileName.substring(0, fileName.length() - 4);
                keysList.add(key);
            }
        }
    }

    public boolean diskKVExists(String key){
        //check list of keys
        return keysList.contains(key);
    }
    public String diskGetKV(String key){
        String value = null;
        try{
            String filePath = getKVPath(key);
            BufferedReader buffReader = new BufferedReader(new FileReader (filePath));
            value = buffReader.readLine();
            buffReader.close();
        } catch (IOException e){
            System.out.println("Error getting from disk");
        }
        return value;
    }
    public void diskPutKV(String key, String value){
        if (diskKVExists(key)){
            diskRemoveKV(key);
        }
        diskAddKV(key, value);
        keysList.add(key);
    }

    public void diskAddKV(String key, String value){
        try{
            String filePath = getKVPath(key);
            FileWriter fileWriter = new FileWriter(filePath);
            fileWriter.write(value);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e){
            System.out.println("Error inserting new KV pair");
        }
    }

    public void diskRemoveKV(String key){
        try{
            String filePath = getKVPath(key);
            File file = new File(filePath);
            file.delete();
            keysList.remove(key);
        } catch (SecurityException e){
            System.err.println("Error deleting key from disk");
        }
    }

    public void deleteDisk(){
        while(!keysList.isEmpty()){
            String key = keysList.get(0);
            diskRemoveKV(key);
        }
        if (KVDiskPath != ""){
            //delete directory if it was created
            File directory = new File(KVDiskPath.substring(0, KVDiskPath.length() - 1));
            if(directory.exists()){
                directory.delete();
            }
        }
    }

    public String getKVPath(String key){
        String KVPath;
        KVPath = KVDiskPath.concat(key + ".txt");

        return KVPath;
    }
    public TreeMap<String, String> getAllKV(){
        TreeMap<String, String> allKVs = new TreeMap<String, String>();
        for (int i = 0; i < keysList.size(); i++){
            String key = keysList.get(i);
            String value = diskGetKV(key);
            allKVs.put(key, value);
        }
        return allKVs;
    }
    public void createReplicaDirectory(int replicaNum){
        if(replicaNum == 1 || replicaNum == 2){
            if(replicaExists(replicaNum)){
                deleteReplica(replicaNum);
            }
            String ReplicaPath = "replica_" + String.valueOf(replicaNum) + "/";
            if(!this.KVDiskPath.equals(".")){
                ReplicaPath = this.KVDiskPath.concat(ReplicaPath);
            }  
            File directory = new File(ReplicaPath);
            if(!directory.exists()){
                directory.mkdirs();
                switch(replicaNum){
                    case 1:
                        this.KVReplicaOnePath = ReplicaPath;
                    break;
                    case 2:
                        this.KVReplicaTwoPath = ReplicaPath;
                    break;
                    default:
                        //should not reach here, do nothing (max 2 replicas m3)
                    break;
                }
            }
        } else{
            System.out.println("Invalid replica number");
        }
    }

    public String getReplicaPath(String key, int replicaNum){
        String path = null;
        if(replicaExists(replicaNum)){
            switch(replicaNum){
                case 1:
                    path = this.KVReplicaOnePath.concat(key+".txt");
                break;
                case 2:
                    path = this.KVReplicaTwoPath.concat(key+".txt");
                break;
                default:
                break;
            }
        } else{
            System.out.println("replica" + replicaNum + "does not exist, cannot get KV path.");
        }
        return path;
    }
    //adds a KV to the replica #replicaNum
    public void replicaAddKV(String key, String value, int replicaNum){
        String path = getReplicaPath(key, replicaNum);
        if(path != null){
            try{
                FileWriter fileWriter = new FileWriter(path);
                fileWriter.write(value);
                fileWriter.flush();
                fileWriter.close();
            }
            catch (IOException e){
                System.out.println("Error inserting new replica KV pair");
            }
        } else{
            System.out.println("Invalid replica number");
        }
    }
    public boolean replicaExists(int replicaNum){
        //returns true if the replica exists else false (decide if need another var or if can just use pathVars)
        boolean exists = false;
        if(replicaNum == 1 || replicaNum == 2){
            switch(replicaNum){
                case 1:
                    if (this.KVReplicaOnePath != null ){
                        exists = true;
                    }
                break;
                case 2:
                    if (this.KVReplicaTwoPath != null ){
                        exists = true;
                    }
                break;
                default:
                break;
            }
        } else{
            System.out.println("Invalid replica number");
        }
        return exists;
    }

    public void replicaAddKV(int replicaNum, String key, String value){
        //case where a key is added to another server and that server coordinates its replicas by sending the new KV
        try{
            String filePath = getReplicaPath(key, replicaNum);
            if(value == null || value == "null"){ //case where KV is deleted
                File file = new File(filePath);
                file.delete();
            } else{
                FileWriter fileWriter = new FileWriter(filePath);
                fileWriter.write(value);
                fileWriter.flush();
                fileWriter.close();
            }
        } catch (IOException e){
            System.out.println("Error inserting new KV pair into Replica");
            //not sure if this replaces file or appends (if appends will prob have to check if file exists delete file and then call FileWriter, for KV update case)
        }
    }

    public void insertReplicaIntoDisk(int replicaNum){
        //case where a server shutsdown and we use the replica to restore, should probably also add code to delete the replica or at least remove all KVs
        //System.out.println("Inserting Replica into disk");
        if(replicaNum == 1 || replicaNum == 2){
            if(replicaExists(replicaNum)){
                String replicaPath;
                if(replicaNum == 1){
                    replicaPath = this.KVReplicaOnePath;
                } else{
                    replicaPath = this.KVReplicaTwoPath;
                }
                //System.out.println("Replica is at this file path:" + replicaPath);
                File replicaFolder = new File(replicaPath);
                File[] listOfFiles = replicaFolder.listFiles();
                for (int i = 0; i < listOfFiles.length; i++) {
                    if (listOfFiles[i].isFile()) {
                        try{
                            String key = listOfFiles[i].getName();
                            key = key.substring(0, key.length() - 4);
                            BufferedReader buffReader = new BufferedReader(new FileReader (listOfFiles[i]));
                            String value = buffReader.readLine();
                            buffReader.close();
                            System.out.println("Inserting KV:" + key + value + "into disk");
                            diskPutKV(key, value);
                        } catch (IOException e){
                            System.out.println("Error reading/transferring from replica to disk");
                        }
                    } 
                }
            } 
        } else{
            System.out.println("Invalid replica number");
        }
    }
    

    public void deleteReplica(int replicaNum){
        //delete specific replica
        if(replicaNum == 1 || replicaNum == 2){
            if(replicaExists(replicaNum)){
                String replicaPath;
                if(replicaNum == 1){
                    replicaPath = this.KVReplicaOnePath;
                    this.KVReplicaOnePath = null;
                } else{
                    replicaPath = this.KVReplicaTwoPath;
                    this.KVReplicaTwoPath = null;
                }
                File replicaFolder = new File(replicaPath);
                for(File file: replicaFolder.listFiles()){
                    if (!file.isDirectory()){ 
                        file.delete();
                    }
                }
                //delete folder here
                File directory = new File(replicaPath.substring(0, replicaPath.length() - 1));
                if(directory.exists()){
                    directory.delete();
                }
            }
        } else{
            System.out.println("Invalid replica number");
        }
    }

    public void deleteReplicas(){
        //code to delete both replicas on shutdown
        deleteReplica(1);
        deleteReplica(2);
    }
    //maybe don't need this if we just replace existing replica 
    public void replica2Promotion(){
        //code to move replica 2 into replica 1 case where server two infront gets deleted
        if(replicaExists(1) && replicaExists(2)){
        }
    }
    //prob not needed (if implemented in replica2Promotion)
    public TreeMap <String, String> replicaGetAll(int replicaNum){
        //returns a treemap of all the KVs in the replicae (not sure if this is needed)
        return null;
    }

}



