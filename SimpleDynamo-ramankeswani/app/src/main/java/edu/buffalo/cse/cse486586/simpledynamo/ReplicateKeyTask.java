package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;

/**
 * Created by Keswani on 4/18/2018.
 */

public class ReplicateKeyTask extends Thread {

    String sucPort, key, value, myPort, nextSuc;
    boolean response;
    MissedData missedData;

    ReplicateKeyTask(String sucPort, String key, String value, String myPort, String nextSuc) {

        this.key = key;
        this.sucPort = sucPort;
        this.value = value;
        this.myPort = myPort;
        this.nextSuc = nextSuc;
    }

    @Override
    public void run() {
        Log.v("cp", "ReplicateKeyTask Starts");
        boolean result = false;
        Log.v("cp", "ReplicateKeyTask sucPort: " + sucPort + " key: " + key + " value: " + value + " myPort: " + myPort);
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sucPort));
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeUTF(myPort);
            objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_COORD_REQ);
            objectOutputStream.writeUTF(key);
            objectOutputStream.writeUTF(value);
            objectOutputStream.flush();
            Log.v("cp", "ReplicateKeyTask sent request. Now waiting");
            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            String replyFromPort = objectInputStream.readUTF();
            String messageType = objectInputStream.readUTF();
            String resultRemote = objectInputStream.readUTF();
            Log.v("cp","ReplicateKeyTask resultRemote: " + resultRemote + " replyFromPort: " + replyFromPort + " messageType: " +messageType);
            if(replyFromPort.compareTo(sucPort) == 0 && messageType.compareTo(SimpleDynamoConstants.MESSAGE_COORD_REQ_REPLY) == 0) {
                result = (SimpleDynamoConstants.SUCCESS.compareTo(resultRemote) == 0) ? Boolean.TRUE : Boolean.FALSE;
            } else {
                Log.v("cp","ReplicateKeyTask ERROR Unexpected reply");
                result = Boolean.FALSE;
            }
        } catch (IOException e) {
            missedData = new MissedData(key,value,sucPort);
            Log.v("cp", "ReplicateKeyTask IOException: " + e.toString());
            Log.v("cp", "ReplicateKeyTask IOException missedData.key: " + missedData.key + " missedData.sucPort: " + missedData.port);
        }
        Log.v("cp", "ReplicateKeyTask Received reply from 1st successor result: " + result + " nextSuc: " + nextSuc);
        response = result;
        Log.v("cp", "ReplicateKeyTask Ends");
    }

    public boolean getResponse() {
        return response;
    }

    public MissedData getMissedData() {
        return missedData;
    }
}
