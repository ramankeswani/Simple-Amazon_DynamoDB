package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import org.w3c.dom.Node;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Keswani on 4/19/2018.
 */

public class StartUpCheckTask extends AsyncTask<String, Void, KeyValuePair> {

    @Override
    protected KeyValuePair doInBackground(String... strings) {
        String myPort = strings[0];
        List<NodeData> ringList = initialiseRingList();
        Log.v("cp", "StartUpCheckTask Start myPort: " + myPort);
        Socket[] sockets = new Socket[4];
        List<String> keys = new ArrayList<String>();
        List<String> values = new ArrayList<String>();
        int i = 0, keyValMissed = 0;
        for (NodeData node : ringList
                ) {
            try {
                Log.v("cp", "StartUpCheckTask Request to " + node.port);
                if (myPort.compareTo(node.port) != 0) {
                    sockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.port));
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(sockets[i].getOutputStream());
                    objectOutputStream.writeUTF(myPort);
                    objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_STARTUP_CHECK);
                    objectOutputStream.flush();
                    Log.v("cp", "StartUpCheckTask Request Completed to " + node.port);
                    i++;
                }
            } catch (IOException e) {
                Log.v("cp", " StartUpCheckTask Request  IOException node.port: " + node.port);
            }
        }
        Log.v("cp", "StartUpCheckTask Reading Response Now");
        for (i = 0; i < 4; i++) {
            try {
                if(null != sockets[i]) {
                    ObjectInputStream objectInputStream = new ObjectInputStream(sockets[i].getInputStream());
                    String remotePort = objectInputStream.readUTF();
                    String messageType = objectInputStream.readUTF();
                    keyValMissed = objectInputStream.readInt();
                    Log.v("cp", "StartUpCheckTask Read remotePort: " + remotePort + " messageType: " + messageType + " keyValMissed: " + keyValMissed);
                    for (int j = 0; j < keyValMissed; j++) {
                        keys.add(objectInputStream.readUTF());
                        values.add(objectInputStream.readUTF());
                        Log.v("cp", "StartUpCheckTask Read keys[j]: " + keys.get(j) + " values[j]: " + values.get(j));
                    }
                }
            } catch (IOException e) {
                if (keys.size() != 0) {
                    String[] keysArr = keys.toArray(new String[keys.size()]);
                    String[] valuesArr = values.toArray(new String[values.size()]);
                    Log.v("cp", "StartUpCheckTask End");
                    return new KeyValuePair(keysArr, valuesArr);
                }
                Log.v("cp", "StartUpCheckTask IOException: " + e.toString());
            }
        }
        if (keys.size() != 0) {
            String[] keysArr = keys.toArray(new String[keys.size()]);
            String[] valuesArr = values.toArray(new String[values.size()]);
            Log.v("cp", "StartUpCheckTask End");
            return new KeyValuePair(keysArr, valuesArr);
        } else {
            Log.v("cp", "StartUpCheckTask End");
            return null;
        }
    }

    private List<NodeData> initialiseRingList() {
        Log.v("cp", "RingHelper initialiseRingList Starts");
        List<NodeData> ringList = new ArrayList<NodeData>();
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11124, SimpleDynamoConstants.HASH_11124));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11112, SimpleDynamoConstants.HASH_11112));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11108, SimpleDynamoConstants.HASH_11108));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11116, SimpleDynamoConstants.HASH_11116));
        ringList.add(new NodeData(SimpleDynamoConstants.PORT_11120, SimpleDynamoConstants.HASH_11120));
        Log.v("cp", "RingHelper initialiseRingList Ends");
        return ringList;
    }
}
