package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int firstNode = 5554;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String LOCAL_DHT = "@";
    private static final String ALL_DHT = "*";
    static final int nodeJoinRequest = 1;
    static final int updateNeighborRequest = 2;
    static final int forwardKeyValueRequest = 3;
    static final int getValueForKeyRequest = 4;
    static final int getAllKeysRequest = 5;
    static final int deleteKeyRequest = 6;
    private final Uri mUri;
    static int prevNode;
    static int nextNode;
    static String myPort;
    static String myPortgenHash;
    static int myPortInt;
    boolean amIAlone;
    boolean serverRequest;
    static int sourceNode;
    ArrayList<Nodes> listOfNodes;
    ArrayList<String> fileList;

    public SimpleDhtProvider() {
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String query = selection;
        if(amIAlone) {
            if(query.equals(ALL_DHT) || query.equals(LOCAL_DHT)) {
                for(String temp: fileList) {
                    getContext().deleteFile(temp);
                }
                fileList.clear();
                return 0;
            } else {
                int i = 0;
                boolean didbreak = false;
                for(i = 0; i < fileList.size(); i++) {
                    if(fileList.get(i).equals(query)) {
                        getContext().deleteFile(query);
                        didbreak = true;
                        break;
                    }
                }
                if(didbreak) {
                    fileList.remove(i);
                    return 0;
                }
                return 1;
            }
        } else {
            if(query.equals(ALL_DHT)) {

            } else if (query.equals(LOCAL_DHT)) {
                for(String temp: fileList) {
                    getContext().deleteFile(temp);
                }
                fileList.clear();
                return 0;
            } else {
                try {
                    String querygenHash = genHash(query);
                    String nextnodegenHash = genHash(Integer.toString(nextNode));
                    String prevnodegenHash = genHash(Integer.toString(prevNode));
                    if (myPortgenHash.compareTo(prevnodegenHash) > 0) {
                        if(myPortgenHash.compareTo(querygenHash) >= 0 && querygenHash.compareTo(prevnodegenHash) > 0) {
                            int i = 0;
                            boolean didbreak = false;
                            for(i = 0; i < fileList.size(); i++) {
                                if(fileList.get(i).equals(query)) {
                                    getContext().deleteFile(query);
                                    didbreak = true;
                                    break;
                                }
                            }
                            if(didbreak) {
                                fileList.remove(i);
                                return 0;
                            }
                            return 1;
                        } else {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(deleteKeyRequest), query);
                        }
                    } else {
                        if(querygenHash.compareTo(prevnodegenHash) > 0 || myPortgenHash.compareTo(querygenHash) >= 0) {
                            int i = 0;
                            boolean didbreak = false;
                            for(i = 0; i < fileList.size(); i++) {
                                if(fileList.get(i).equals(query)) {
                                    getContext().deleteFile(query);
                                    didbreak = true;
                                    break;
                                }
                            }
                            if(didbreak) {
                                fileList.remove(i);
                                return 0;
                            }
                            return 1;
                        } else {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(deleteKeyRequest), query);
                        }

                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, e.toString());
                }

            }

        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString(KEY_FIELD);
        String keygenHash = null;
        try{
             keygenHash = genHash(key);
        } catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Exception : " + e.toString());
        }
        String val = values.getAsString(VALUE_FIELD);
        if(amIAlone){
            FileOutputStream outputStream;
            try {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(val.getBytes());
                fileList.add(key);
                //Log.d(TAG,"Key : " + key + " Value : " + val);
            } catch (IOException e) {
                Log.e(TAG, e.toString());
            } catch (NullPointerException e) {
                Log.e(TAG, e.toString());
            }
            return uri;
        } else {
            try {
                String nextnodegenHash = genHash(Integer.toString(nextNode));
                String prevnodegenHash = genHash(Integer.toString(prevNode));
                if(myPortgenHash.compareTo(prevnodegenHash) > 0) {
                    if(myPortgenHash.compareTo(keygenHash) >= 0 && keygenHash.compareTo(prevnodegenHash) > 0) {
                        FileOutputStream outputStream;
                        try {
                            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            outputStream.write(val.getBytes());
                            fileList.add(key);
                            //Log.d(TAG,"Key : " + key + " Value : " + val);
                        } catch (IOException e) {
                            Log.e(TAG, e.toString());
                        } catch (NullPointerException e) {
                            Log.e(TAG, e.toString());
                        }
                    } else {
                        //Log.d(TAG,"Forewards Key : " + key + " Value : " + val + "to "+ nextNode);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(forwardKeyValueRequest), key, val);
                    }
                } else {
                    if(keygenHash.compareTo(prevnodegenHash) > 0 || myPortgenHash.compareTo(keygenHash) >= 0) {
                        FileOutputStream outputStream;
                        try {
                            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            outputStream.write(val.getBytes());
                            fileList.add(key);
                            //Log.d(TAG,"Key : " + key + " Value : " + val);
                        } catch (IOException e) {
                            Log.e(TAG, e.toString());
                        } catch (NullPointerException e) {
                            Log.e(TAG, e.toString());
                        }
                    } else {
                        //Log.d(TAG,"Forewards Key : " + key + " Value : " + val + "to "+ nextNode);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(forwardKeyValueRequest), key, val);
                    }
                }
            } catch (NoSuchAlgorithmException e){
                Log.e(TAG, e.toString());
            }
            return uri;

        }

    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)));
        try {
            myPortgenHash = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Exception : " + e.toString());
        }
        myPortInt =  Integer.parseInt(portStr);
        amIAlone = false;
        serverRequest = false;
        fileList = new ArrayList<String>();
        //Log.d(TAG, "Value of my port : " + myPortInt);

        if(myPortInt == 5554) {
            try {
                amIAlone = true;
                Nodes initializer = new Nodes(myPortInt, genHash(myPort));
                listOfNodes = new ArrayList<Nodes>();
                listOfNodes.add(initializer);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Exception : "+ e.toString());
            }
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        if(myPortInt != 5554){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(nodeJoinRequest));
        }
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        String query = selection;
        if(amIAlone) {
            if(query.equals(ALL_DHT) || query.equals(LOCAL_DHT)) {
                String result = "";
                MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                FileInputStream inputStream;
                try {
                    for(String temp : fileList) {
                        inputStream = getContext().openFileInput(temp);
                        if (inputStream != null) {
                            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                            result = br.readLine();
                            //Log.d(TAG, "Query Returning result :"  + result);
                            matrixCursor.addRow(new Object[] {temp, result});
                            inputStream.close();
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return matrixCursor;
            } else {
                String result = "";
                MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                FileInputStream inputStream;
                try {
                    inputStream = getContext().openFileInput(query);
                    if (inputStream != null) {
                        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                        result = br.readLine();
                        //Log.d(TAG, "Query Returning result :"  + result);
                        matrixCursor.addRow(new Object[] {query, result});
                        inputStream.close();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return matrixCursor;
            }
        } else {
            if(query.equals(ALL_DHT)) {
                //Log.d(TAG, "My file list :" + Arrays.toString(fileList.toArray()));
                String result = "";
                MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                FileInputStream inputStream;
                try {
                    for(String temp : fileList) {
                        inputStream = getContext().openFileInput(temp);
                        if (inputStream != null) {
                            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                            result = br.readLine();
                            //Log.d(TAG, "Query Returning result :"  + result);
                            matrixCursor.addRow(new Object[] {temp, result});
                            inputStream.close();
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(!serverRequest || (serverRequest && nextNode != sourceNode)) {
                    try {
                        String queryResult;
                        if(serverRequest) {
                            queryResult = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(getAllKeysRequest), Integer.toString(sourceNode)).get();
                        } else {
                            queryResult = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(getAllKeysRequest), myPort).get();
                        }
                        serverRequest = false;
                        //Log.d(TAG, "Entire queryResult : " + queryResult);
                        if(!queryResult.isEmpty()) {
                            String keyvaluelist[] = queryResult.split(":#:");
                            for(String keyvalue : keyvaluelist) {
                                String currentkeyval[] = keyvalue.split(":%:");
                                //Log.d(TAG, "currentkeyval length : " + currentkeyval.length);
                                matrixCursor.addRow(new Object[] {currentkeyval[0], currentkeyval[1]});
                            }
                        }

                    } catch (InterruptedException e) {
                        Log.e(TAG, e.toString());
                    } catch (ExecutionException e) {
                        Log.e(TAG, e.toString());
                    }
                } else {
                    //Log.d(TAG, "Serverrequest : "+ serverRequest);
                    //Log.d(TAG, "next node : "+ nextNode);
                    //Log.d(TAG, "source node : "+ sourceNode);
                    if(serverRequest) {
                        serverRequest = false;
                    }
                }
                return matrixCursor;
            } else if (query.equals(LOCAL_DHT)) {
                String result = "";
                MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                FileInputStream inputStream;
                try {
                    for(String temp : fileList) {
                        inputStream = getContext().openFileInput(temp);
                        if (inputStream != null) {
                            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                            result = br.readLine();
                            //Log.d(TAG, "Query Returning result :"  + result);
                            matrixCursor.addRow(new Object[] {temp, result});
                            inputStream.close();
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return matrixCursor;

            } else {
                try {
                    String querygenHash = genHash(query);
                    String nextnodegenHash = genHash(Integer.toString(nextNode));
                    String prevnodegenHash = genHash(Integer.toString(prevNode));
                    if (myPortgenHash.compareTo(prevnodegenHash) > 0) {
                        if(myPortgenHash.compareTo(querygenHash) >= 0 && querygenHash.compareTo(prevnodegenHash) > 0) {
                            String result = "";
                            MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                            FileInputStream inputStream;
                            try {
                                inputStream = getContext().openFileInput(query);
                                if (inputStream != null) {
                                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                                    result = br.readLine();
                                    //Log.d(TAG, "Query Returning result :"  + result);
                                    matrixCursor.addRow(new Object[] {query, result});
                                    inputStream.close();
                                }
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return matrixCursor;
                        } else {
                            try {
                                String queryResult = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(getValueForKeyRequest), query).get();
                                MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                                if(!queryResult.isEmpty()) {
                                    String keyvalue[] = queryResult.split(":%:");
                                    //Log.d(TAG, "Query Result "+ keyvalue[0] + " and " + keyvalue[1]);
                                    matrixCursor.addRow(new Object[] {keyvalue[0], keyvalue[1]});
                                }
                                return matrixCursor;
                            } catch (InterruptedException e) {
                                Log.e(TAG, "Exception : "+ e.toString());
                            } catch (ExecutionException e) {
                                Log.e(TAG, "Exception : "+ e.toString());
                            }
                        }

                    } else {
                        if(querygenHash.compareTo(prevnodegenHash) > 0 || myPortgenHash.compareTo(querygenHash) >= 0) {
                            String result = "";
                            MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                            FileInputStream inputStream;
                            try {
                                inputStream = getContext().openFileInput(query);
                                if (inputStream != null) {
                                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                                    result = br.readLine();
                                    //Log.d(TAG, "Query Returning result :"  + result);
                                    matrixCursor.addRow(new Object[] {query, result});
                                    inputStream.close();
                                }
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return matrixCursor;
                        } else {
                            try {
                                String queryResult = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(getValueForKeyRequest), query).get();
                                MatrixCursor matrixCursor = new MatrixCursor(new String[] {KEY_FIELD, VALUE_FIELD});
                                if(!queryResult.isEmpty()) {
                                    String keyvalue[] = queryResult.split(":%:");
                                    //Log.d(TAG, "Query Result "+ keyvalue[0] + " and " + keyvalue[1]);
                                    matrixCursor.addRow(new Object[] {keyvalue[0], keyvalue[1]});
                                }
                                return matrixCursor;
                            } catch (InterruptedException e) {
                                Log.e(TAG, "Exception : "+ e.toString());
                            } catch (ExecutionException e) {
                                Log.e(TAG, "Exception : "+ e.toString());
                            }
                        }

                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, e.toString());
                }

            }
        }
        //Log.d(TAG, "Returning null");
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private class ServerTask extends AsyncTask<ServerSocket, Integer, Void> {
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            String recvdMsg[];
            while (true) {
                try {
                    Socket server = serverSocket.accept();
                    DataInputStream in = new DataInputStream(server.getInputStream());
                    DataOutputStream out = new DataOutputStream(server.getOutputStream());
                    recvdMsg = in.readUTF().split(":#:");

                    switch (Integer.parseInt(recvdMsg[0])) {
                        case nodeJoinRequest:
                            if(myPortInt == 5554) {
                                try {
                                    amIAlone = false;
                                    int prevNodeOfClient = -1;
                                    int nextNodeOfClient = -1;
                                    String hashedValue = genHash(recvdMsg[1]);
                                    Nodes current = new Nodes(Integer.parseInt(recvdMsg[1]), hashedValue);
                                    boolean inserted = false;
                                    for (int i = 0; i < listOfNodes.size(); i++) {
                                        Nodes nodeAti = listOfNodes.get(i);
                                        if (hashedValue.compareTo(nodeAti.hash) < 0) {
                                            if(i!=0){
                                                prevNodeOfClient = listOfNodes.get(i-1).node;
                                            } else {
                                                prevNodeOfClient = listOfNodes.get(listOfNodes.size()-1).node;
                                            }
                                            nextNodeOfClient = nodeAti.node;
                                            listOfNodes.add(i, current);
                                            inserted = true;
                                            break;
                                        }
                                    }
                                    if (!inserted) {
                                        prevNodeOfClient = listOfNodes.get(listOfNodes.size()-1).node;
                                        nextNodeOfClient = listOfNodes.get(0).node;
                                        listOfNodes.add(current);
                                    }

                                    out.writeUTF(prevNodeOfClient+":#:"+nextNodeOfClient);
                                    out.flush();
                                    if(!in.readUTF().equals("OK")) {
                                        Log.e(TAG, "Did not receive ok");
                                    }
                                    publishProgress(prevNodeOfClient, nextNodeOfClient, Integer.parseInt(recvdMsg[1]));
                                    out.close();
                                    in.close();
                                    server.close();

                                } catch (NoSuchAlgorithmException e) {
                                    Log.e(TAG, "Exception : " + e.toString());
                                }
                            } else {
                                Log.e(TAG, "This Log should never be printed");
                            }
                            break;

                        case updateNeighborRequest:
                            if(Integer.parseInt(recvdMsg[1]) == 1) {
                                prevNode = Integer.parseInt(recvdMsg[2]);
                                //Log.d(TAG, "Prev Node : " + prevNode);
                                out.writeUTF("OK");
                                out.flush();
                                out.close();
                                in.close();
                                server.close();
                            } else if(Integer.parseInt(recvdMsg[1]) == 2) {
                                nextNode = Integer.parseInt(recvdMsg[2]);
                                //Log.d(TAG, "Next Node : " + nextNode);
                                out.writeUTF("OK");
                                out.flush();
                                out.close();
                                in.close();
                                server.close();
                            }
                            break;

                        case forwardKeyValueRequest:
                            ContentValues mContentValues = new ContentValues();
                            mContentValues.put(KEY_FIELD, recvdMsg[1]);
                            mContentValues.put(VALUE_FIELD, recvdMsg[2]);
                            getContext().getContentResolver().insert(mUri, mContentValues);
                            out.writeUTF("OK");
                            out.flush();
                            out.close();
                            in.close();
                            server.close();
                            break;

                        case getValueForKeyRequest:
                            String key = recvdMsg[1];
                            String result = "";
                            boolean first_iteration = true;
                            Cursor resultCursor = getContext().getContentResolver().query(mUri, null,
                                    key, null, null);
                            while (resultCursor.moveToNext()) {
                                if(first_iteration) {
                                    result = resultCursor.getString(resultCursor.getColumnIndex(KEY_FIELD));
                                    first_iteration = false;
                                } else {
                                    result = result + ":#:" + resultCursor.getString(resultCursor.getColumnIndex(KEY_FIELD));
                                }
                                result = result + ":%:" + resultCursor.getString(resultCursor.getColumnIndex(VALUE_FIELD));
                            }
                            resultCursor.close();
                            out.writeUTF(result);
                            if(!in.readUTF().equals("OK")) {
                                Log.e(TAG, "Did not receive ok");
                            }
                            out.close();
                            in.close();
                            server.close();
                            break;

                        case getAllKeysRequest:
                            boolean first_iteration1 = true;
                            sourceNode = Integer.parseInt(recvdMsg[1]);
                            serverRequest = true;
                            String result1 = "";
                            Cursor resultCursor1 = getContext().getContentResolver().query(mUri, null,
                                    ALL_DHT, null, null);
                            while (resultCursor1.moveToNext()) {
                                if(first_iteration1) {
                                    result1 = resultCursor1.getString(resultCursor1.getColumnIndex(KEY_FIELD));
                                    first_iteration1 = false;
                                } else {
                                    result1 = result1 + ":#:" + resultCursor1.getString(resultCursor1.getColumnIndex(KEY_FIELD));
                                }
                                result1 = result1 + ":%:" + resultCursor1.getString(resultCursor1.getColumnIndex(VALUE_FIELD));
                            }
                            resultCursor1.close();
                            //Log.d(TAG, "Server sends this : " + result1 + " to node " + prevNode);
                            out.writeUTF(result1);
                            if(!in.readUTF().equals("OK")) {
                                Log.e(TAG, "Did not receive ok");
                            }
                            out.close();
                            in.close();
                            server.close();
                            break;

                        case deleteKeyRequest:
                            getContext().getContentResolver().delete(mUri, recvdMsg[1], null);
                            out.writeUTF("OK");
                            out.flush();
                            out.close();
                            in.close();
                            server.close();
                            break;

                        default:
                            break;
                    }

                } catch (IOException e) {
                    Log.e(TAG, "Servertask socket IOException");
                }
            }
        }

        protected void onProgressUpdate(Integer... neighbors){
            new ServerInformerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, neighbors);
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            switch (Integer.parseInt(msgs[0])) {
                case nodeJoinRequest:
                    if (myPortInt != 5554) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    firstNode * 2);
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            out.writeUTF(nodeJoinRequest + ":#:" + myPort);
                            out.flush();
                            String msg[] = in.readUTF().split(":#:");
                            out.writeUTF("OK");
                            out.flush();
                            prevNode = Integer.parseInt(msg[0]);
                            nextNode = Integer.parseInt(msg[1]);
                            //Log.d(TAG, "Prev Node : " + prevNode);
                            //Log.d(TAG, "Next Node : " + nextNode);
                            out.close();
                            in.close();
                            socket.close();
                        } catch (EOFException e) {
                            amIAlone = true;
                            Log.e(TAG, "Exception1 : " + e.toString());
                        } catch (UnknownHostException e) {
                            Log.e(TAG, "Unknown Host Exception");
                        } catch (IOException e) {
                            Log.e(TAG, "Exception : " + e.toString());
                        }
                    } else {
                        Log.e(TAG,"This log should never be printed");
                    }
                    break;

                case forwardKeyValueRequest:
                    //Log.d(TAG,"Now Forwarding Key : " + msgs[1] + " Value : " + msgs[2] + "to "+ nextNode);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                nextNode * 2);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(forwardKeyValueRequest + ":#:" + msgs[1] + ":#:" + msgs[2]);
                        out.flush();
                        String recvd = in.readUTF();
                        if(!recvd.equals("OK")) {
                            Log.e(TAG, "Received " + recvd + "instead of OK");
                        }
                        out.close();
                        in.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    }
                    break;

                case getValueForKeyRequest:
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                nextNode * 2);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(getValueForKeyRequest + ":#:" + msgs[1]);
                        out.flush();
                        String result = in.readUTF();
                        out.writeUTF("OK");
                        out.flush();
                        out.close();
                        in.close();
                        socket.close();
                        return result;
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    }
                    break;
                case getAllKeysRequest:
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                nextNode * 2);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(getAllKeysRequest + ":#:" + msgs[1]);
                        out.flush();
                        String result = in.readUTF();
                        //Log.d(TAG, "Client receives this : " + result + " from node " + nextNode);
                        out.writeUTF("OK");
                        out.flush();
                        out.close();
                        in.close();
                        socket.close();
                        return result;
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    }
                    break;

                case deleteKeyRequest:
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                nextNode * 2);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(deleteKeyRequest + ":#:" + msgs[1]);
                        out.flush();
                        String recvd = in.readUTF();
                        if(!recvd.equals("OK")) {
                            Log.e(TAG, "Received " + recvd + "instead of OK");
                        }
                        out.close();
                        in.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    } catch (IOException e) {
                        Log.e(TAG, "Exception : " + e.toString());
                    }
            }

            return null;
        }
    }

    private class ServerInformerTask extends AsyncTask<Integer, Void, Void> {
        protected Void doInBackground(Integer... neighbors) {
            try {
                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        neighbors[0]*2);
                DataInputStream in1 = new DataInputStream(socket1.getInputStream());
                DataOutputStream out1 = new DataOutputStream(socket1.getOutputStream());
                out1.writeUTF(updateNeighborRequest + ":#:" + 2 + ":#:" + neighbors[2]);
                out1.flush();
                String recvd1 = in1.readUTF();
                if(!recvd1.equals("OK")) {
                    Log.e(TAG, "Received " + recvd1 + "instead of OK");
                }
                out1.close();
                in1.close();
                socket1.close();

                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        neighbors[1]*2);
                DataInputStream in2 = new DataInputStream(socket2.getInputStream());
                DataOutputStream out2 = new DataOutputStream(socket2.getOutputStream());
                out2.writeUTF(updateNeighborRequest + ":#:" + 1 + ":#:" + neighbors[2]);
                out2.flush();
                String recvd2 = in2.readUTF();
                if(!recvd2.equals("OK")) {
                    Log.e(TAG, "Received " + recvd2 + "instead of OK");
                }
                out2.close();
                in2.close();
                socket2.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "Exception : "+e.toString());
            } catch (IOException e) {
                Log.e(TAG, "Exception : "+e.toString());
            }
            return null;
        }
    }

    private class Nodes {
        int node;
        String hash;
        Nodes(int node, String hash){
            this.node = node;
            this.hash = hash;
        }
    }
}
