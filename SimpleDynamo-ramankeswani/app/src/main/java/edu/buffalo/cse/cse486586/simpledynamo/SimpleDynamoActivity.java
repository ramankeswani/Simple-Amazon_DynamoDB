package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	TextView tv;
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		passMyNode();

		Button star = (Button) findViewById(R.id.Star);
		star.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				ContentValues contentValues = new ContentValues();
				contentValues.put(SimpleDynamoConstants.COLUMN_KEY, "UVlRsd5dMBJPT2MbQKZ6yGbf6ZADvk0O");
				contentValues.put(SimpleDynamoConstants.COLUMN_VALUE, "kzQcFkPo5Bb81ugrNLWkJinsmzSDGPPt");
				/*contentValues.put(SimpleDynamoConstants.COLUMN_REPLICA_MISSED_BY, "11116");
				contentValues.put(SimpleDynamoConstants.INSERT_BY, SimpleDynamoConstants.FAILURE_HANDLER);*/
				Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
				try {
					getContentResolver().insert(uri, contentValues);
				} catch (Exception e) {
					Log.v("cp", "InsertHelper callInsertMethod Exception: " + e.toString());
				}
			}
		});

		Button qAt = (Button) findViewById(R.id.QAT);
		qAt.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
				Cursor resultCursor = getContentResolver().query(mUri, new String[]{SimpleDynamoConstants.COLUMN_KEY, SimpleDynamoConstants.COLUMN_VALUE},
						"@", null, null);
				resultCursor.moveToFirst();
				for(int i=0; i<resultCursor.getCount(); i++) {
					if(resultCursor.getString(0).compareTo("raman") == 0) {
						tv.append("-------------------------------------------------------------------");
					}
					tv.append(resultCursor.getString(0) + " " + resultCursor.getString(1));
					tv.append("\n");
					resultCursor.moveToNext();
				}
			}
		});
	}

	public void passMyNode(){
		Log.v("cp", "SimpleDynamoActivity passMyNode Starts");
		TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		Log.v("cp", "SimpleDynamoActivity passMyNode myNode_ID: " + portStr);
		SimpleDynamoProvider.setMyNode(portStr);
		Log.v("cp", "SimpleDynamoActivity passMyNode Ends");
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

}
