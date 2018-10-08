package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by Keswani on 4/25/2018.
 */

public class ReplicateOnNextTask extends Thread {

    String sucPort, key, value, myPort, nextSuc;
    boolean response;
    MissedData missedData;

    ReplicateOnNextTask(String sucPort, String key, String value, String myPort, String nextSuc) {
        this.key = key;
        this.sucPort = sucPort;
        this.value = value;
        this.myPort = myPort;
        this.nextSuc = nextSuc;
    }

    @Override
    public void run() {
        Log.v("cp", "ReplicateOnNextTask Starts");
        boolean result = false;
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextSuc));
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeUTF(myPort);
            objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_COORD_REQ);
            objectOutputStream.writeUTF(key);
            objectOutputStream.writeUTF(value);
            objectOutputStream.flush();
            Log.v("cp", "ReplicateOnNextTask sent request. Now waiting");
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            String replyFromPort = objectInputStream.readUTF();
            String messageType = objectInputStream.readUTF();
            String resultRemote = objectInputStream.readUTF();
            Log.v("cp", "ReplicateOnNextTask resultRemote: " + resultRemote + " replyFromPort: " + replyFromPort + " messageType: " + messageType);
            if (replyFromPort.compareTo(nextSuc) == 0 && messageType.compareTo(SimpleDynamoConstants.MESSAGE_COORD_REQ_REPLY) == 0) {
                result = (SimpleDynamoConstants.SUCCESS.compareTo(resultRemote) == 0) ? Boolean.TRUE : Boolean.FALSE;
            } else {
                Log.v("cp", "ReplicateOnNextTask ERROR Unexpected reply");
                result = Boolean.FALSE;
            }
        } catch (IOException e) {
            missedData = new MissedData(key, value, nextSuc);
            Log.v("cp", "ReplicateOnNextTask IOException: " + e.toString());
            Log.v("cp", "ReplicateOnNextTask IOException missedData.key: " + missedData.key + " missedData.sucPort: " + missedData.port);
        }
        Log.v("cp", "ReplicateOnNextTask Received reply from 2nd successor result: " + result);
        response = result;
        Log.v("cp", "ReplicateOnNextTask Ends");
    }

    public boolean getResponse() {
        return response;
    }

    public MissedData getMissedData() {
        return missedData;
    }
}
