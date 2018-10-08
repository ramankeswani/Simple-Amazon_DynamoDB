package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.util.Log;

/**
 * Created by Keswani on 4/18/2018.
 */

public class InsertHelper {

    public boolean callInsertMethodAsCoordinator(String key, String value, ContentResolver contentResolver) {
        Log.v("cp", "InsertHelper callInsertMethodAsCoordinator Starts key: " + key + " value: " + value);
        ContentValues contentValues = new ContentValues();
        contentValues.put(SimpleDynamoConstants.COLUMN_KEY, key);
        contentValues.put(SimpleDynamoConstants.COLUMN_VALUE,value);
        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        try {
            contentResolver.insert(uri, contentValues);
        } catch (Exception e) {
            Log.v("cp", "InsertHelper callInsertMethodAsCoordinator Exception: " + e.toString());
            return false;
        }
        Log.v("cp", "InsertHelper callInsertMethodAsCoordinator Ends");
        return true;
    }

    public boolean callInsertMethod(String key, String value, ContentResolver contentResolver) {
        Log.v("cp", "InsertHelper callInsertMethod Starts key: " + key + " value: " + value);
        ContentValues contentValues = new ContentValues();
        contentValues.put(SimpleDynamoConstants.COLUMN_KEY, key);
        contentValues.put(SimpleDynamoConstants.COLUMN_VALUE,value);
        contentValues.put(SimpleDynamoConstants.INSERT_BY,SimpleDynamoConstants.INSERT_DYNAMO);
        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        try {
            contentResolver.insert(uri, contentValues);
        } catch (Exception e) {
            Log.v("cp", "InsertHelper callInsertMethod Exception: " + e.toString());
            return false;
        }
        Log.v("cp", "InsertHelper callInsertMethod Ends");
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
