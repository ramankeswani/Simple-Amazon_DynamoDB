package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by Keswani on 4/19/2018.
 */

public class QueryHelper {

    ContentResolver myContentResolver;

    QueryHelper (ContentResolver contentResolver) {
        this.myContentResolver = contentResolver;
    }

    public String portToQuery(String portCoord) {
        Log.v("cp", "QueryHelper portToQuery Starts portCoord: " + portCoord);
        String portToQuery = "";
        if(portCoord.compareTo(SimpleDynamoConstants.PORT_11108) == 0){
            portToQuery =  SimpleDynamoConstants.PORT_11120;
        } else if(portCoord.compareTo(SimpleDynamoConstants.PORT_11112) == 0){
            portToQuery =  SimpleDynamoConstants.PORT_11116;
        } else if(portCoord.compareTo(SimpleDynamoConstants.PORT_11116) == 0){
            portToQuery =  SimpleDynamoConstants.PORT_11124;
        } else if(portCoord.compareTo(SimpleDynamoConstants.PORT_11120) == 0){
            portToQuery =  SimpleDynamoConstants.PORT_11112;
        } else if(portCoord.compareTo(SimpleDynamoConstants.PORT_11124) == 0){
            portToQuery =  SimpleDynamoConstants.PORT_11108;
        }
        Log.v("cp", "QueryHelper portToQuery Ends portToQuery: " + portToQuery);
        return portToQuery;
    }

    public MatrixCursor queryRemoteSingleKey(String port, String key, String myPort) {
        MatrixCursor matrixCursor = null;
        Log.v("cp", "QueryHelper queryRemoteSingleKey Starts port: " + port + " key: " + key + " myPort: " + myPort);
        try {
            String[] keyValPair = new QueryRemoteSingleKeyTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port, key, myPort).get();
            if(null == keyValPair) {
                Log.v("cp", "QueryHelper queryRemoteSingleKey Null from Last Node");
                keyValPair = getDataFromMiddleNode(key, port, myPort);
            }
            String[] columns = new String[]{SimpleDynamoConstants.COLUMN_KEY, SimpleDynamoConstants.COLUMN_VALUE};
            matrixCursor = new MatrixCursor(columns);
            matrixCursor.addRow(new Object[]{keyValPair[0], keyValPair[1]});
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (Exception e) {
            Log.v("cp", "QueryHelper queryRemoteSingleKey Exception: " + e.toString());
        }
        Log.v("cp", "QueryHelper queryRemoteSingleKey Ends (null != matrixCursor): " + (null != matrixCursor));
        return (null != matrixCursor) ? matrixCursor : null;
    }

    private class QueryRemoteSingleKeyTask extends AsyncTask<String, Void, String[]>  {

        @Override
        protected String[] doInBackground(String... strings) {
            Log.v("cp","QueryHelper QueryRemoteSingleKeyTask Starts");
            String port = strings[0], key = strings[1], myPort = strings[2], keyReply = "", valueReply = "";
            Log.v("cp", "QueryHelper QueryRemoteSingleKeyTask port: " + port + " key: " + key + " myPort: " + myPort);
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeUTF(myPort);
                objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_QUERY_SINGLE_KEY);
                objectOutputStream.writeUTF(key);
                objectOutputStream.flush();
                Log.v("cp", "QueryHelper QueryRemoteSingleKeyTask Sent request. Now waiting");
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                String remotePort = objectInputStream.readUTF();
                String messageType = objectInputStream.readUTF();
                keyReply = objectInputStream.readUTF();
                valueReply = objectInputStream.readUTF();
                Log.v("cp","QueryHelper QueryRemoteSingleKeyTask Received remotePort: " + remotePort +" keyReply: " + keyReply + " valueReply: " + valueReply + " messageType: " + messageType);
            } catch (IOException e) {
                Log.v("cp", "QueryHelper QueryRemoteSingleKeyTask IOException: " + e.toString());
                return null;
            }
            Log.v("cp","QueryHelper QueryRemoteSingleKeyTask Ends");
            return new String[]{keyReply, valueReply};
        }
    }

    private class QueryRemoteSingleKeyOverrideTask extends AsyncTask<String, Void, String[]>  {

        @Override
        protected String[] doInBackground(String... strings) {
            Log.v("cp","QueryHelper QueryRemoteSingleKeyTask Starts");
            String port = strings[0], key = strings[1], myPort = strings[2], keyReply = "", valueReply = "";
            Log.v("cp", "QueryHelper QueryRemoteSingleKeyTask port: " + port + " key: " + key + " myPort: " + myPort);
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeUTF(myPort);
                objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_QUERY_SINGLE_KEY_OVERRIDE);
                objectOutputStream.writeUTF(key);
                objectOutputStream.flush();
                Log.v("cp", "QueryHelper QueryRemoteSingleKeyTask Sent request. Now waiting");
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                String remotePort = objectInputStream.readUTF();
                String messageType = objectInputStream.readUTF();
                keyReply = objectInputStream.readUTF();
                valueReply = objectInputStream.readUTF();
                Log.v("cp","QueryHelper QueryRemoteSingleKeyTask Received remotePort: " + remotePort +" keyReply: " + keyReply + " valueReply: " + valueReply + " messageType: " + messageType);
            } catch (IOException e) {
                Log.v("cp", "QueryHelper QueryRemoteSingleKeyTask from port: " + port + " IOException: " + e.toString());
                Log.v("cp","QueryHelper QueryRemoteSingleKeyTask Ends with Exception");
                return null;
            }
            Log.v("cp","QueryHelper QueryRemoteSingleKeyTask Ends");
            return new String[]{keyReply, valueReply};
        }
    }

    private String[] getDataFromMiddleNode(String key, String failedPort, String myPort) {
        Log.v("cp", "QueryHelper getDataFromMiddleNode Starts failedPort: " + failedPort + " myPort: " + myPort + " key: " + key);
        String middleNode = getMiddleNodePort(failedPort);
        Log.v("cp", "QueryHelper getDataFromMiddleNode middleNode: " + middleNode);
        if(myPort.compareTo(middleNode) == 0) {
            String[] keyValPair = callQueryMethodOverride(key,myContentResolver);
            Log.v("cp", "QueryHelper getDataFromMiddleNode I middle node Ends");
            return keyValPair;
        } else {
            try {
                String[] keyValPair = new QueryRemoteSingleKeyOverrideTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,middleNode, key, myPort).get();
                if(null == keyValPair) {
                    Log.v("cp", "QueryHelper getDataFromMiddleNode Null from Middle Node");
                    keyValPair = getDataFromCoord(key, middleNode, myPort);
                }
                Log.v("cp", "QueryHelper getDataFromMiddleNode  got data from middle node Ends");
                return keyValPair;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        Log.v("cp", "QueryHelper getDataFromMiddleNode Ends");
        return null;
    }

    private String[] getDataFromCoord(String key, String middleNode, String myPort) {
        Log.v("cp", "QueryHelper getDataFromCoord Starts failedPort middleNode: " + middleNode + " myPort: " + myPort + " key: " + key);
        String coordNode = getMiddleNodePort(middleNode); //Also serves for getting ooord node
        Log.v("cp", "QueryHelper getDataFromCoord coordNode: " + coordNode);
        if(myPort.compareTo(coordNode) == 0) {
            String[] keyValPair = callQueryMethodOverride(key,myContentResolver);
            Log.v("cp", "QueryHelper getDataFromCoord I coord node Ends");
            return keyValPair;
        } else {
            try {
                String[] keyValPair = new QueryRemoteSingleKeyOverrideTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,coordNode, key, myPort).get();
                Log.v("cp", "QueryHelper getDataFromCoord  got data from Coord node Ends");
                return keyValPair;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        Log.v("cp", "QueryHelper getDataFromCoord Ends Failed");
        return null;
    }

    private String getMiddleNodePort(String failedPort) {
        String middleNode;
        if(failedPort.compareTo(SimpleDynamoConstants.PORT_11108) == 0) {
            middleNode = SimpleDynamoConstants.PORT_11112;
        } else if(failedPort.compareTo(SimpleDynamoConstants.PORT_11112) == 0) {
            middleNode = SimpleDynamoConstants.PORT_11124;
        } else if(failedPort.compareTo(SimpleDynamoConstants.PORT_11116) == 0) {
            middleNode = SimpleDynamoConstants.PORT_11108;
        } else if(failedPort.compareTo(SimpleDynamoConstants.PORT_11120) == 0) {
            middleNode = SimpleDynamoConstants.PORT_11116;
        } else {
            middleNode = SimpleDynamoConstants.PORT_11120;
        }
        return middleNode;
    }
    public MatrixCursor queryStar(String myPort, List<NodeData> ringList, ContentResolver contentResolver) {
        Log.v("cp", "QueryHelper queryStar Starts");
        String[] columns = new String[]{SimpleDynamoConstants.COLUMN_KEY, SimpleDynamoConstants.COLUMN_VALUE};
        MatrixCursor matrixCursor = new MatrixCursor(columns);
        Cursor ownCursor = queryAllAtSelf(contentResolver);
        int keyIndex = ownCursor.getColumnIndex(SimpleDynamoConstants.COLUMN_KEY);
        int valueIndex = ownCursor.getColumnIndex(SimpleDynamoConstants.COLUMN_VALUE);
        ownCursor.moveToFirst();
        Log.v("cp", "QueryHelper queryStar ownCursor.getCount(): " + ownCursor.getCount());
        for (int i = 0; i < ownCursor.getCount(); i++) {
            matrixCursor.addRow(new Object[]{ownCursor.getString(keyIndex), ownCursor.getString(valueIndex)});
            ownCursor.moveToNext();
        }
        Log.v("cp", "QueryHelper queryStar Completed Self Query");
        try {
            for (NodeData nodeData : ringList
                    ) {
                if (myPort.compareTo(nodeData.port) != 0) {
                    Log.v("cp", "QueryHelper queryStar Request to: " + nodeData.port);
                    KeyValuePair keyValuePair = new QueryStarTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,nodeData.port, myPort).get();
                    Log.v("cp", "QueryHelper queryStar Response from: " + nodeData.port + " length: " + keyValuePair.keys.length);
                    for (int i=0; i<keyValuePair.keys.length; i++) {
                        matrixCursor.addRow(new Object[]{keyValuePair.keys[i], keyValuePair.values[i]});
                    }
                }
            }
        } catch (InterruptedException e) {
            Log.v("cp", "QueryHelper queryStar InterruptedException: " + e.toString());
        } catch (ExecutionException e) {
            Log.v("cp", "QueryHelper queryStar ExecutionException: " + e.toString());
        }
        Log.v("cp", "QueryHelper queryStar Ends");
        return matrixCursor;
    }

    private class QueryStarTask extends AsyncTask<String, Void, KeyValuePair> {

        @Override
        protected KeyValuePair doInBackground(String... strings) {
            Log.v("cp", "QueryStarTask Starts");
            String remotePort = strings[0], myPort = strings[1];
            String[] keys, values;
            KeyValuePair keyValuePair;
            Log.v("cp", "QueryStarTask remotePort: " + remotePort + " myPort: " + myPort);
            try{
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeUTF(myPort);
                objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_QUERY_STAR);
                objectOutputStream.flush();
                Log.v("cp", "QueryStarTask Sent. Now Waiting");
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                String remotePortReply = objectInputStream.readUTF();
                String messageType = objectInputStream.readUTF();
                int messageCount = objectInputStream.readInt();
                Log.v("cp", "QueryStarTask remotePortReply: " + remotePortReply + " messageType: " + messageType + " messageCount: " + messageCount);
                keys = new String[messageCount];
                values = new String[messageCount];
                for (int i = 0; i < messageCount; i++) {
                    keys[i] = objectInputStream.readUTF();
                    values[i] = objectInputStream.readUTF();
                }
                keyValuePair = new KeyValuePair(keys, values);
            } catch (IOException e) {
                Log.v("cp", "QueryStarTask IOException: " + e.toString());
                return new KeyValuePair(new String[0], new String[0]);
            }
            Log.v("cp", "QueryStarTask Ends");
            return keyValuePair;
        }
    }

    public Cursor queryAllAtSelf(ContentResolver contentResolver) {
        Log.v("cp", "QueryHelper queryAllAtSelf Starts");
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        Cursor resultCursor = contentResolver.query(mUri, null, SimpleDynamoConstants.QUERY_ALL_AT_SELF, null, null);
        Log.v("cp", "QueryHelper queryAllAtSelf Ends");
        return  resultCursor;
    }

    public String[] callQueryMethod(String key, ContentResolver mContentResolver) {
        Log.v("cp", "QueryHelper callQueryMethod Starts key: " + key);
        String[] keyValPair = new String[2];
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        Cursor resultCursor = mContentResolver.query(mUri, null, key, null, null);
        if (resultCursor == null) {
            Log.v("cp", "QueryHelper ERROR Result null");
            return null;
        } else  {
            keyValPair = cursorToString(resultCursor);
            Log.v("cp", "QueryHelper callQueryMethod key: " + keyValPair[0] + " value: " + keyValPair[1]);
        }
        Log.v("cp","QueryHelper callQueryMethod Ends");
        return keyValPair;
    }

    public String[] callQueryMethodOverride(String key, ContentResolver mContentResolver) {
        Log.v("cp", "QueryHelper callQueryMethodOverride Starts key: " + key);
        String[] keyValPair = new String[2];
        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        Cursor resultCursor = mContentResolver.query(mUri, null, SimpleDynamoConstants.QUERY_OVERRIDE, new String[]{key}, null);
        if (resultCursor == null) {
            Log.v("cp", "QueryHelper callQueryMethodOverride ERROR Result null");
            return null;
        } else  {
            keyValPair = cursorToString(resultCursor);
            Log.v("cp", "QueryHelper callQueryMethodOverride key: " + keyValPair[0] + " value: " + keyValPair[1]);
        }
        Log.v("cp","QueryHelper callQueryMethodOverride Ends");
        return keyValPair;
    }

    private String[] cursorToString(Cursor cursor) {
        String[] response = new String[2];
        int keyIndex = cursor.getColumnIndex(SimpleDynamoConstants.COLUMN_KEY);
        int valueIndex = cursor.getColumnIndex(SimpleDynamoConstants.COLUMN_VALUE);
        cursor.moveToFirst();
        String key = cursor.getString(keyIndex);
        String value = cursor.getString(valueIndex);
        response[0] = key;
        response[1] = value;
        return response;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
