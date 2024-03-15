package KVCache;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList; //concurrent ArrayList implementation 

public class KVCache {
    private ConcurrentHashMap<String, String> KVCache;
    private int cacheSize;
    private List<String> keyLRU;

    //constructor
    public KVCache(int size /*, String cacheStrategy*/){
        this.cacheSize = size;
        this.KVCache = new ConcurrentHashMap<String, String>();
        this.keyLRU = new CopyOnWriteArrayList<String>();
    }

    public String getCache(String key){
        moveToFrontLRU(key);
        return KVCache.get(key);
    }

    public void putCache(String key, String value){
        if(cacheSize > 0){
            if (inCache(key)){
                KVCache.remove(key);
                popFIFO(key);
            }
            if(keyLRU.size() == cacheSize){
                removeLRU();
            }
            KVCache.put(key, value);
            insertLRU(key);
        }
    }

    public void removeCache(String key){
        keyLRU.remove(key);
        KVCache.remove(key);
    }

    public boolean inCache(String key){
        return keyLRU.contains(key);
    }
    private void moveToFrontLRU(String key){
        popFIFO(key);
        insertLRU(key);
    }
    private void insertLRU(String key){
        keyLRU.add(0,key);

    }
    private void popFIFO(String key){
        //if key exists
        keyLRU.remove(key);
        //KVCache.remove(key);
    }
    private void removeLRU(){
        //if cache full remove last index
        String key = keyLRU.get(cacheSize -1);
        keyLRU.remove(cacheSize-1);
        KVCache.remove(key);
    }
    public void clearCache(){
        keyLRU.clear();
        KVCache.clear();
    }

}
