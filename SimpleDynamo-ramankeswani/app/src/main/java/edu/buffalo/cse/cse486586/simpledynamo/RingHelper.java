package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Keswani on 4/18/2018.
 */

public class RingHelper {

    Object lock = new Object();
    public String getKeyCoordinator(String hash, List<NodeData> ringList) {
        Log.v("cp", "RingHelper getKeyCoordinator Starts hash: " + hash);
        String keyCoordinator = null;
        for (NodeData nodeData: ringList
                ) {
            if(hash.compareTo(nodeData.hash) < 0) {
                keyCoordinator = nodeData.port;
                break;
            }
        }
        keyCoordinator = (null != keyCoordinator) ? keyCoordinator : ringList.get(0).port;
        Log.v("cp", "RingHelper getKeyCoordinator keyCoordinator: " + keyCoordinator);
        Log.v("cp", "RingHelper getKeyCoordinator Ends");
        return keyCoordinator;
    }

    public List<NodeData> initialiseRingList() {
        Log.v("cp", "RingHelper initialiseRingList Starts");
        List<NodeData> ringList = new ArrayList<NodeData>();
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11124, SimpleDynamoConstants.HASH_11124));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11112, SimpleDynamoConstants.HASH_11112));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11108, SimpleDynamoConstants.HASH_11108));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11116, SimpleDynamoConstants.HASH_11116));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11120, SimpleDynamoConstants.HASH_11120));
        Log.v("cp", "RingHelper initialiseRingList Ends");
        return  ringList;
    }

    public KeyValuePair getMissedData(String remotePort, List<MissedData> missedDataList) {
        List<String> keys = new ArrayList<String>();
        List<String> values = new ArrayList<String>();
        synchronized (lock) {
            Log.v("cp", "RingHelper getMissedData Starts remotePort: " + remotePort);
            Log.v("cp", "RingHelper getMissedData missedDataList.size(): " + missedDataList.size());
            Iterator<MissedData> iterator = missedDataList.iterator();
            while (iterator.hasNext()){
                MissedData missedData = iterator.next();
                if (remotePort.compareTo(missedData.port) == 0) {
                    Log.v("cp", "RingHelper getMissedData found key: " + missedData.key);
                    keys.add(missedData.key);
                    values.add(missedData.value);
                }
            }
            Log.v("cp", "RingHelper getMissedData keys count:" + keys.size());
            Log.v("cp", "RingHelper getMissedData LATER missedDataList.size(): " + missedDataList.size());
        }
        if(keys.size() == 0) {
            Log.v("cp", "RingHelper getMissedData Ends");
            return null;
        } else {
            String[] keysArr = keys.toArray(new String[keys.size()]);
            String[] valueArr = values.toArray(new String[values.size()]);
            missedDataList.clear();
            Log.v("cp", "RingHelper getMissedData Ends");
            return  new KeyValuePair(keysArr,valueArr);
        }

    }

    public String getNextSuc(String sucPort) {
        Log.v("cp", "RingHelper getNextSuc Starts sucPort: " + sucPort);
        String nextSuc = "";
        if(sucPort.compareTo(SimpleDynamoConstants.PORT_11108) == 0){
            nextSuc =  SimpleDynamoConstants.PORT_11116;
        }
        if(sucPort.compareTo(SimpleDynamoConstants.PORT_11112) == 0){
            nextSuc =  SimpleDynamoConstants.PORT_11108;
        }
        if(sucPort.compareTo(SimpleDynamoConstants.PORT_11116) == 0){
            nextSuc =  SimpleDynamoConstants.PORT_11120;
        }
        if(sucPort.compareTo(SimpleDynamoConstants.PORT_11120) == 0){
            nextSuc =  SimpleDynamoConstants.PORT_11124;
        }
        if(sucPort.compareTo(SimpleDynamoConstants.PORT_11124) == 0){
            nextSuc =  SimpleDynamoConstants.PORT_11112;
        }
        Log.v("cp", "RingHelper getNextSuc Ends nextSuc: " + nextSuc);
        return nextSuc;
    }
}
