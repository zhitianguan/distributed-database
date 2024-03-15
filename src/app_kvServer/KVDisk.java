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
}



