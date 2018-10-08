package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Keswani on 4/17/2018.
 */

public class SimpleDynamoDbHelper extends SQLiteOpenHelper{

    SimpleDynamoDbHelper(Context context){
        super(context, SimpleDynamoConstants.DB_NAME, null,SimpleDynamoConstants.DB_VERSION );
    }

    @Override
    public void onCreate(SQLiteDatabase db){
        String CREATE_TABLE = "CREATE TABLE " + SimpleDynamoConstants.TABLE_NAME + " (" +
                SimpleDynamoConstants.COLUMN_KEY + " TEXT PRIMARY KEY ON CONFLICT REPLACE, " +
                SimpleDynamoConstants.COLUMN_VALUE + " TEXT);";
        db.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int vOld, int vNew){
        db.execSQL("DROP TABLE IF EXISTS " + SimpleDynamoConstants.TABLE_NAME);
        onCreate(db);
    }
}
