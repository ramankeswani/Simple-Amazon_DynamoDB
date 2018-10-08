package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static String myNode;
    static final int SERVER_PORT = 10000;
    String myHash, myPort, sucPort, preDecPort, sucHash, preDecHash;
    List<NodeData> ringList;
    RingHelper ringHelper;
    SimpleDynamoDbHelper dbHelper;
    InsertHelper insertHelper;
    QueryHelper queryHelper;
    List<MissedData> missedDataList;
    Object missedDataListLock = new Object();
    Object startUpLock = new Object();
    int startLock;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.v("cp", "SimpleDynamoProvider delete Starts");
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        int deleteCount = db.delete(SimpleDynamoConstants.TABLE_NAME, SimpleDynamoConstants.COLUMN_KEY + " LIKE ? ", new String[]{selection});
        Log.v("cp", "SimpleDynamoProvider delete count: " + deleteCount);
        getContext().getContentResolver().notifyChange(uri, null);
        Log.v("cp", "SimpleDynamoProvider delete Ends");
        return deleteCount;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.v("cp", "SimpleDynamoProvider insert Starts");
        boolean insertHere = false;
        String key = values.get(SimpleDynamoConstants.COLUMN_KEY).toString();
        String value = values.get(SimpleDynamoConstants.COLUMN_VALUE).toString();
        Log.v("cp", "SimpleDynamoProvider insert key: " + key + " value: " + value);
        String insertPostedBy = (null != values.get(SimpleDynamoConstants.INSERT_BY)) ? values.get(SimpleDynamoConstants.INSERT_BY).toString() : SimpleDynamoConstants.INSERT_GRADER;
        Log.v("cp", "SimpleDynamoProvider insert insertPostedBy: " + insertPostedBy);
        if (SimpleDynamoConstants.INSERT_GRADER.compareTo(insertPostedBy) == 0) {
            try {
                String hash = genHash(values.get(SimpleDynamoConstants.COLUMN_KEY).toString());
                Log.v("cp", "SimpleDynamoProvider insert key: " + key + " hash: " + hash);
                String keyCoordinator = ringHelper.getKeyCoordinator(hash, ringList);
                Log.v("cp", "SimpleDynamoProvider insert keyCoordinator: " + keyCoordinator);
                if (myPort.compareTo(keyCoordinator) == 0) {
                    String nextSuc = ringHelper.getNextSuc(sucPort);
                    Log.v("cp", "SimpleDynamoProvider insert nextSuc: " + nextSuc);
                    ReplicateKeyTask replicateKeyTask = new ReplicateKeyTask(sucPort, key, value, myPort, nextSuc);
                    replicateKeyTask.start();
                    Log.v("cp", "SimpleDynamoProvider ReplicateKeyTask Started");
                    ReplicateOnNextTask replicateOnNextTask = new ReplicateOnNextTask(sucPort, key, value, myPort, nextSuc);
                    replicateOnNextTask.start();
                    Log.v("cp", "SimpleDynamoProvider replicateOnNextTask Started");
                    replicateKeyTask.join();
                    Log.v("cp", "SimpleDynamoProvider ReplicateKeyTask Finished");
                    boolean response = replicateKeyTask.getResponse();
                    MissedData missedData = replicateKeyTask.getMissedData();
                    Log.v("cp", "SimpleDynamoProvider insert (null != missedData): " + (null != missedData));
                    if (null != missedData) {
                        synchronized (missedDataListLock) {
                            Log.v("cp", "SimpleDynamoProvider insert missedDataList Added missedData: " + missedData.key + " " + missedData.port);
                            missedDataList.add(missedData);
                        }
                    }
                    replicateOnNextTask.join();
                    Log.v("cp", "SimpleDynamoProvider replicateOnNextTask Finished");
                    response = response && replicateOnNextTask.getResponse();
                    MissedData missedDataOnNext = replicateOnNextTask.getMissedData();
                    Log.v("cp", "SimpleDynamoProvider insert on next (null != missedDataOnNext): " + (null != missedDataOnNext));
                    if (null != missedDataOnNext) {
                        synchronized (missedDataListLock) {
                            Log.v("cp", "SimpleDynamoProvider insert missedDataList Added missedData: " + missedDataOnNext.key + " " + missedDataOnNext.port);
                            missedDataList.add(missedDataOnNext);
                        }
                    }
                    Log.v("cp", "SimpleDynamoProvider insert keyCoordinator response: " + response);
                    Log.v("cp", "SimpleDynamoProvider insert As coord: key: " + key);
                    insertHere = true;
                } else {
                    Log.v("cp", "SimpleDynamoProvider insert NOT COORD Starts");
                    ForwardInsToCoordTask forwardInsToCoordTask = new ForwardInsToCoordTask(keyCoordinator, key, value, myPort, getContext().getContentResolver());
                    forwardInsToCoordTask.start();
                    Log.v("cp", "SimpleDynamoProvider insert ForwardInsToCoordTask Started");
                    forwardInsToCoordTask.join();
                    Log.v("cp", "SimpleDynamoProvider insert ForwardInsToCoordTask Finished");
                    boolean resultRemote = forwardInsToCoordTask.getResponse();
                    Log.v("cp", "SimpleDynamoProvider insert Im not coord: resultRemote: " + resultRemote);
                    MissedData missedData = forwardInsToCoordTask.getMissedData();
                    Log.v("cp", "SimpleDynamoProvider insert null == missedData: " + (null == missedData));
                    if (null != missedData) {
                        synchronized (missedDataListLock) {
                            Log.v("cp", "SimpleDynamoProvider insert added missedData:  " + missedData.port + " " + missedData.key + " missedData.value: " + missedData.value);
                            missedDataList.add(missedData);
                        }
                        HandleCoordinatorFailed handleCoordinatorFailed = new HandleCoordinatorFailed(myPort, keyCoordinator, key, value);
                        handleCoordinatorFailed.start();
                        Log.v("cp", "ForwardInsToCoordTask IOException handleCoordinatorFailed Started");
                        try {
                            handleCoordinatorFailed.join();
                            Log.v("cp", "ForwardInsToCoordTask IOException handleCoordinatorFailed Completed");
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                    Log.v("cp", "SimpleDynamoProvider insert NOT COORD Ends");
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else if (SimpleDynamoConstants.INSERT_DYNAMO.compareTo(insertPostedBy) == 0) {
            Log.v("cp", "SimpleDynamoProvider insert As Replicate: key: " + key);
            values.remove(SimpleDynamoConstants.INSERT_BY);
            insertHere = true;
        }

        if (insertHere) {
            Log.v("cp", "SimpleDynamoProvider insert Actual Insert Happening");
            SQLiteDatabase db = dbHelper.getWritableDatabase();
            long id = db.insert(SimpleDynamoConstants.TABLE_NAME, null, values);
            if (id <= 0) {
                throw new SQLException("could not insert Content provider");
            }
            Log.v("cp", "ContentProvider Inserted: key: " + key + "value: " + value);
            getContext().getContentResolver().notifyChange(uri, null);
        }
        Log.v("cp", "SimpleDynamoProvider insert Ends");
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.v("cp", "SimpleDynamoProvider onCreate Starts");
        startLock = 1;
        ringHelper = new RingHelper();
        new SetupTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
        Context context = getContext();
        dbHelper = new SimpleDynamoDbHelper(context);
        insertHelper = new InsertHelper();
        queryHelper = new QueryHelper(getContext().getContentResolver());
        synchronized (missedDataListLock) {
            missedDataList = new ArrayList<MissedData>();
        }
        Log.v("cp", "SimpleDynamoProvider onCreate Ends");
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            Log.v("cp", "ServerTask Starts");
            ServerSocket serverSocket = serverSockets[0];
            try {
                while (true) {
                    Log.v("cp", "ServerTask Ready To Accept");
                    Socket socket = serverSocket.accept();
                    Log.v("cp", "ServerTask Accepted Connection");
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    String remotePort = objectInputStream.readUTF();
                    String messageType = objectInputStream.readUTF();
                    Log.v("cp", "ServerTask remotePort: " + remotePort + " messageType: " + messageType);
                    if (SimpleDynamoConstants.MESSAGE_FORWARD_TO_COORD.compareTo(messageType) == 0) {
                        Log.v("cp", "ServerTask MESSAGE_FORWARD_TO_COORD Starts");
                        String key = objectInputStream.readUTF();
                        String value = objectInputStream.readUTF();
                        Log.v("cp", "MESSAGE_FORWARD_TO_COORD key: " + key + " value: " + value);
                        new HandleForwardToCoord(key, value, myPort, getContext().getContentResolver(), socket).start();
                        Log.v("cp", "ServerTask MESSAGE_FORWARD_TO_COORD Ends");
                    } else if (SimpleDynamoConstants.MESSAGE_COORD_REQ.compareTo(messageType) == 0) {
                        Log.v("cp", "ServerTask MESSAGE_COORD_REQ Starts");
                        String key = objectInputStream.readUTF();
                        String value = objectInputStream.readUTF();
                        Log.v("cp", "MESSAGE_COORD_REQ key: " + key + " value: " + value);
                        boolean insertStatus = insertHelper.callInsertMethod(key, value, getContext().getContentResolver());
                        Log.v("cp", "MESSAGE_COORD_REQ insertStatus: " + insertStatus);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        String insertResponse = (insertStatus) ? SimpleDynamoConstants.SUCCESS : SimpleDynamoConstants.FAILURE;
                        objectOutputStream.writeUTF(myPort);
                        objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_COORD_REQ_REPLY);
                        objectOutputStream.writeUTF(insertResponse);
                        objectOutputStream.flush();
                        Log.v("cp", "MESSAGE_COORD_REQ reply myPort: " + myPort + " insertResponse: " + insertResponse);
                        Log.v("cp", "ServerTask MESSAGE_COORD_REQ Ends");
                    } else if (SimpleDynamoConstants.MESSAGE_QUERY_SINGLE_KEY.compareTo(messageType) == 0) {
                        Log.v("cp", "ServerTask MESSAGE_QUERY_SINGLE_KEY Starts");
                        String key = objectInputStream.readUTF();
                        Log.v("cp", "MESSAGE_QUERY_SINGLE_KEY key: " + key);
                        String[] keyValPair = queryHelper.callQueryMethod(key, getContext().getContentResolver());
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeUTF(myPort);
                        objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_QUERY_SINGLE_KEY_REPLY);
                        objectOutputStream.writeUTF(keyValPair[0]);
                        objectOutputStream.writeUTF(keyValPair[1]);
                        objectOutputStream.flush();
                        Log.v("cp", "MESSAGE_QUERY_SINGLE_KEY reply keyValPair[0]: " + keyValPair[0] + " keyValPair[1]: " + keyValPair[1]);
                        Log.v("cp", "ServerTask MESSAGE_QUERY_SINGLE_KEY Ends");
                    } else if (SimpleDynamoConstants.MESSAGE_QUERY_STAR.compareTo(messageType) == 0) {
                        Log.v("cp", "ServerTask MESSAGE_QUERY_STAR Starts");
                        Cursor cursor = queryHelper.queryAllAtSelf(getContext().getContentResolver());
                        int keyIndex = cursor.getColumnIndex(SimpleDynamoConstants.COLUMN_KEY);
                        int valueIndex = cursor.getColumnIndex(SimpleDynamoConstants.COLUMN_VALUE);
                        int count = cursor.getCount();
                        Log.v("cp", "ServerTask MESSAGE_QUERY_STAR count: " + count);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeUTF(myPort);
                        objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_QUERY_STAR_REPLY);
                        objectOutputStream.writeInt(count);
                        cursor.moveToFirst();
                        for (int i = 0; i < count; i++) {
                            objectOutputStream.writeUTF(cursor.getString(keyIndex));
                            objectOutputStream.writeUTF(cursor.getString(valueIndex));
                            cursor.moveToNext();
                        }
                        objectOutputStream.flush();
                        Log.v("cp", "ServerTask MESSAGE_QUERY_STAR Ends");
                    } else if (SimpleDynamoConstants.MESSAGE_STARTUP_CHECK.compareTo(messageType) == 0) {
                        Log.v("cp", "ServerTask MESSAGE_STARTUP_CHECK Starts + remotePort: " + remotePort);
                        int count;
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeUTF(myPort);
                        objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_STARTUP_CHECK_REPLY);
                        KeyValuePair keyValuePair = null;
                        synchronized (missedDataListLock) {
                            if (missedDataList.size() == 0) {
                                count = 0;
                            } else {
                                keyValuePair = ringHelper.getMissedData(remotePort, missedDataList);
                                count = (null != keyValuePair) ? keyValuePair.keys.length : 0;
                            }
                        }
                        Log.v("cp", "ServerTask MESSAGE_STARTUP_CHECK count: " + count);
                        objectOutputStream.writeInt(count);
                        for (int i = 0; i < count; i++) {
                            Log.v("cp", "keyValuePair.keys[i]: " + keyValuePair.keys[i] + " keyValuePair.values[i]: " + keyValuePair.values[i]);
                            objectOutputStream.writeUTF(keyValuePair.keys[i]);
                            objectOutputStream.writeUTF(keyValuePair.values[i]);
                        }
                        objectOutputStream.flush();
                        Log.v("cp", "ServerTask MESSAGE_STARTUP_CHECK Ends");
                    } else if (SimpleDynamoConstants.MESSAGE_QUERY_SINGLE_KEY_OVERRIDE.compareTo(messageType) == 0) {
                        Log.v("cp", "ServerTask MESSAGE_QUERY_SINGLE_KEY Starts");
                        String key = objectInputStream.readUTF();
                        Log.v("cp", "MESSAGE_QUERY_SINGLE_KEY key: " + key);
                        String[] keyValPair = queryHelper.callQueryMethodOverride(key, getContext().getContentResolver());
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeUTF(myPort);
                        objectOutputStream.writeUTF(SimpleDynamoConstants.MESSAGE_QUERY_SINGLE_KEY_OVERRIDE_REPLY);
                        objectOutputStream.writeUTF(keyValPair[0]);
                        objectOutputStream.writeUTF(keyValPair[1]);
                        objectOutputStream.flush();
                        Log.v("cp", "MESSAGE_QUERY_SINGLE_KEY reply keyValPair[0]: " + keyValPair[0] + " keyValPair[1]: " + keyValPair[1]);
                        Log.v("cp", "ServerTask MESSAGE_QUERY_SINGLE_KEY Ends");
                    }
                }
            } catch (IOException e) {
                Log.v("cp", "ServerTask IOException " + e.toString());
            }
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        do {
            int temp;
            synchronized (startUpLock) {
                temp = startLock;
                if(temp == 1) {
                    break;
                }
            }
        }while(true);
        Log.v("cp", "SimpleDynamoProvider query Starts selection: " + selection);
        if (SimpleDynamoConstants.QUERY_ALL_AT_SELF.compareTo(selection) == 0) {
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            Cursor cursor = db.query(SimpleDynamoConstants.TABLE_NAME, new String[]{SimpleDynamoConstants.COLUMN_KEY, SimpleDynamoConstants.COLUMN_VALUE},
                    null, null, null, null, sortOrder);
            cursor.setNotificationUri(getContext().getContentResolver(), uri);
            return cursor;
        } else if (SimpleDynamoConstants.QUERY_ALL.compareTo(selection) == 0) {
            Log.v("cp", "SimpleDynamoProvider query QUERY_ALL Starts");
            MatrixCursor matrixCursor = queryHelper.queryStar(myPort, ringList, getContext().getContentResolver());
            Log.v("cp", "SimpleDynamoProvider query QUERY_ALL Ends");
            return matrixCursor;
        } else if (selection.compareTo(SimpleDynamoConstants.QUERY_OVERRIDE) == 0) {
            Log.v("cp", "SimpleDynamoProvider query QUERY_OVERRIDE Starts");
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            Cursor cursor = db.query(SimpleDynamoConstants.TABLE_NAME, new String[]{SimpleDynamoConstants.COLUMN_KEY, SimpleDynamoConstants.COLUMN_VALUE},
                    SimpleDynamoConstants.COLUMN_KEY + " LIKE ? ", selectionArgs, null, null, sortOrder);
            cursor.setNotificationUri(getContext().getContentResolver(), uri);
            Log.v("cp", "SimpleDynamoProvider query QUERY_OVERRIDE Ends");
            return cursor;
        } else {
            try {
                String hash = genHash(selection);
                Log.v("cp", "SimpleDynamoProvider query Starts selection: " + selection + " hash: " + hash);
                String coordLocation = ringHelper.getKeyCoordinator(hash, ringList);
                String portToQuery = queryHelper.portToQuery(coordLocation);
                Log.v("cp", "SimpleDynamoProvider query coordLocation: " + coordLocation + " portToQuery: " + portToQuery);
                if (myPort.compareTo(portToQuery) == 0) {
                    Log.v("cp", "SimpleDynamoProvider query at self starts");
                    SQLiteDatabase db = dbHelper.getReadableDatabase();
                    Cursor cursor = db.query(SimpleDynamoConstants.TABLE_NAME, new String[]{SimpleDynamoConstants.COLUMN_KEY, SimpleDynamoConstants.COLUMN_VALUE},
                            SimpleDynamoConstants.COLUMN_KEY + " LIKE ? ", new String[]{selection}, null, null, sortOrder);
                    cursor.setNotificationUri(getContext().getContentResolver(), uri);
                    Log.v("cp", "SimpleDynamoProvider query at self null!=cursor: " + (null != cursor));
                    return cursor;
                } else {
                    Log.v("cp", "SimpleDynamoProvider query on portToQuery: " + portToQuery);
                    return queryHelper.queryRemoteSingleKey(portToQuery, selection, myPort);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Log.v("cp", "SimpleDynamoProvider query Ends");
        // }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class SetupTask extends AsyncTask<Void, Void, Void> {

        @Override
        protected Void doInBackground(Void... voids) {
            Log.v("cp", "SetupTask Starts");
            ringList = ringHelper.initialiseRingList();
            while (null == myNode) {
                continue;
            }
            myPort = String.valueOf((Integer.parseInt(myNode) * 2));
            sucPort = ringHelper.getNextSuc(myPort);
            try {
                myHash = genHash(myNode);
                Log.v("cp", "SetupTask myPort: " + myPort + " myNode: " + myNode + " myHash: " + myHash);
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
                synchronized (startUpLock) {
                    startLock = 0;
                }
                KeyValuePair keyValuePairMissed = new StartUpCheckTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, myPort).get();
                Log.v("cp", "ServerTask null != keyValuePairMissed: " + (null != keyValuePairMissed));
                if (null != keyValuePairMissed) {
                    Log.v("cp", "ServerTask keyValuePairMissed.keys.length: " + (keyValuePairMissed.keys.length));
                    for (int i = 0; i < keyValuePairMissed.keys.length; i++) {
                        insertHelper.callInsertMethod(keyValuePairMissed.keys[i], keyValuePairMissed.values[i], getContext().getContentResolver());
                    }
                }
                synchronized (startUpLock) {
                    startLock = 1;
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (IOException e) {
                Log.v("cp", "SetupTask IOException: " + e.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            Log.v("cp", "SetupTask Ends");
            return null;
        }
    }

    static void setMyNode(String myNode) {
        SimpleDynamoProvider.myNode = myNode;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
