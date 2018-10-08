package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TabHost;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;

/**
 * Created by Keswani on 4/18/2018.
 */

public class ForwardInsToCoordTask extends Thread {

    String remotePort, key, value, myPort;
    boolean response;
    ContentResolver contentResolver;
    MissedData missedData;

    ForwardInsToCoordTask(String remotePort, String key, String value, String myPort, ContentResolver contentResolver) {

        this.key = key;
        this.remotePort = remotePort;
        this.value = value;
        this.contentResolver = contentResolver;
        this.myPort = myPort;
    }

    @Override
    public void run() {

        Log.v("cp", "ForwardInsToCoordTask Starts");
        boolean result = false;
        Log.v("cp", "ForwardInsToCoordTask remotePort: " + remotePort + " key: " + key + " value: " + value + " myPort: " + myPort);
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeUTF(myPort);
            objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_FORWARD_TO_COORD);
            objectOutputStream.writeUTF(key);
            objectOutputStream.writeUTF(value);
            objectOutputStream.flush();
            Log.v("cp", "ForwardInsToCoordTask sent request. Now waiting");
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            String replyFromPort = objectInputStream.readUTF();
            String messageType = objectInputStream.readUTF();
            String resultRemote = objectInputStream.readUTF();
            Log.v("cp", "ForwardInsToCoordTask resultRemote: " + resultRemote + " replyFromPort: " + replyFromPort + " messageType: " + messageType);
            if (replyFromPort.compareTo(remotePort) == 0 && messageType.compareTo(SimpleDynamoConstants.MESSAGE_FORWARD_TO_COORD_REPLY) == 0) {
                result = (SimpleDynamoConstants.SUCCESS.compareTo(resultRemote) == 0) ? Boolean.TRUE : Boolean.FALSE;
            } else {
                Log.v("cp", "ForwardInsToCoordTask ERROR Unexpected reply");
                result = Boolean.FALSE;
            }
        } catch (IOException e) {
            Log.v("cp", "ForwardInsToCoordTask Starts IOException: " + e.toString());
            missedData = new MissedData(key,value,remotePort);
            Log.v("cp", "ForwardInsToCoordTask Ends IOException: " + e.toString());
        }
        Log.v("cp", "ForwardInsToCoordTask Ends");
    }

    public MissedData getMissedData() {
        return missedData;
    }

    public boolean getResponse() {
        return response;
    }
}
