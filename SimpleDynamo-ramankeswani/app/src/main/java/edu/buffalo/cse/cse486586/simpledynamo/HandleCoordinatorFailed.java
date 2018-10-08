package edu.buffalo.cse.cse486586.simpledynamo;


import android.util.Log;

/**
 * Created by Keswani on 4/25/2018.
 */

public class HandleCoordinatorFailed extends Thread {



    String myPort, failedCoordPort, key, value;
    boolean response;

    HandleCoordinatorFailed(String myPort, String failedCoordPort, String key, String value) {
        this.myPort = myPort;
        this.failedCoordPort = failedCoordPort;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        response = false;
        Log.v("cp", "ForwardInsToCoordTask HandleCoordinatorFailed Starts");
        Log.v("cp", "HandleCoordinatorFailed myPort: " + myPort + " failedCoordPort: " + failedCoordPort + " key: " + key + " value: " + value);
        String[] nextTwoNodes = getNextTwoNodes(failedCoordPort);
        Log.v("cp", "HandleCoordinatorFailed nextTwoNodes: " + nextTwoNodes[0] + " " + nextTwoNodes[1]);
        ReplicateKeyTask replicateKeyTask = new ReplicateKeyTask(nextTwoNodes[0], key, value, myPort, nextTwoNodes[1]);
        replicateKeyTask.start();
        Log.v("cp", "HandleCoordinatorFailed Started Replicate Task");
        ReplicateOnNextTask replicateOnNextTask = new ReplicateOnNextTask(nextTwoNodes[0], key, value, myPort, nextTwoNodes[1]);
        replicateOnNextTask.start();
        Log.v("cp", "HandleCoordinatorFailed Started ReplicateOnNextTask");
        try {
            replicateKeyTask.join();
            Log.v("cp", "HandleCoordinatorFailed Finished Replicate Task");
            boolean responseFromReplicateTask = replicateKeyTask.getResponse();
            Log.v("cp", "HandleCoordinatorFailed responseFromReplicateTask: " + responseFromReplicateTask);
            response = responseFromReplicateTask;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        try {
            replicateOnNextTask.join();
            Log.v("cp", "HandleCoordinatorFailed Finished ReplicateOnNextTask");
            boolean responseFromReplicateNextTask = replicateOnNextTask.getResponse();
            Log.v("cp", "HandleCoordinatorFailed responseFromReplicateOnNextTask: " + responseFromReplicateNextTask);
            response = response && responseFromReplicateNextTask;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Log.v("cp", "ForwardInsToCoordTask HandleCoordinatorFailed Ends");
    }

    public boolean getResponse() {
        return response;
    }

    private String[] getNextTwoNodes(String port) {

        if (SimpleDynamoConstants.PORT_11108.compareTo(port) == 0) {
            return new String[]{SimpleDynamoConstants.PORT_11116, SimpleDynamoConstants.PORT_11120};
        } else if (SimpleDynamoConstants.PORT_11112.compareTo(port) == 0) {
            return new String[]{SimpleDynamoConstants.PORT_11108, SimpleDynamoConstants.PORT_11116};
        } else if (SimpleDynamoConstants.PORT_11116.compareTo(port) == 0) {
            return new String[]{SimpleDynamoConstants.PORT_11120, SimpleDynamoConstants.PORT_11124};
        } else if (SimpleDynamoConstants.PORT_11120.compareTo(port) == 0) {
            return new String[]{SimpleDynamoConstants.PORT_11124, SimpleDynamoConstants.PORT_11112};
        } else if(SimpleDynamoConstants.PORT_11124.compareTo(port) == 0) {
            return new String[]{SimpleDynamoConstants.PORT_11112, SimpleDynamoConstants.PORT_11108};
        }
        return null;
    }


}
