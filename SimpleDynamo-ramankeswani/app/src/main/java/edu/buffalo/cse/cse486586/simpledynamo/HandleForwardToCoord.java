package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by Keswani on 4/18/2018.
 */

public class HandleForwardToCoord extends Thread {

    String key, value, myPort;
    ContentResolver contentResolver;
    Socket socket;

    HandleForwardToCoord(String key, String value, String myPort, ContentResolver contentResolver, Socket socket) {
        this.key = key;
        this.value = value;
        this.myPort = myPort;
        this.contentResolver = contentResolver;
        this.socket = socket;
    }

    @Override
    public void run() {
        Log.v("cp", "HandleForwardToCoord Starts");
        boolean insertStatus = new InsertHelper().callInsertMethodAsCoordinator(key, value, contentResolver);
        Log.v("cp", "HandleForwardToCoord insertStatus: " + insertStatus);
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            String insertResponse = (insertStatus) ? SimpleDynamoConstants.SUCCESS : SimpleDynamoConstants.FAILURE;
            objectOutputStream.writeUTF(myPort);
            objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_FORWARD_TO_COORD_REPLY);
            objectOutputStream.writeUTF(insertResponse);
            objectOutputStream.flush();
            Log.v("cp", "HandleForwardToCoord reply myPort: " + myPort + " insertResponse: " + insertResponse);
        } catch (IOException e) {
            Log.v("cp", "HandleForwardToCoord IOException: " + e.toString());
        }
        Log.v("cp", "HandleForwardToCoord Ends");
    }
}
