package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.StringTokenizer;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
	private String myPort;
	private String SingleQueryAns;
	private  String SelfSingleQueryAns;
	private  String queryall;
//	private String dead = null;

	//Mapping of Port ID
	HashMap<String, String> portId = new HashMap<String, String>();
	//Making the ring
	ArrayList<String> chord = new ArrayList<String>();


	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.e("Delete:", selection);
		if (selection.equals("@")){
			deletelocal();
		}
		else if (selection.equals("*")){
			Log.e("Del star", selection);
			deletelocal();
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DeleteAll");
		} else if (selection != null){
			Log.e("delete Single:", selection);
			String node = FindNode(selection);
			if (node.equals(myPort)){
				deleteSingleFile(selection);
			} else {
				new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "deleteSingle-" + node + "-" + selection);
			}
		}

		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		Log.e("Key-value", "key:" + key +"," + "value:" + value);
		String node = FindNode(key);
		SendToInsert(key, value, node);
		return null;
	}

	@Override
	public boolean onCreate() {
		//Add portId
		portId.put("11108", "5554");
		portId.put("11112", "5556");
		portId.put("11116", "5558");
		portId.put("11120", "5560");
		portId.put("11124", "5562");

		//Add to Chord
		chord.add("11124");
		chord.add("11112");
		chord.add("11108");
		chord.add("11116");
		chord.add("11120");

		//Calculate my Port

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);

		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.e("my port", myPort);


		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		}catch(IOException e){
			Log.e(TAG, "Can't create a ServerSocket");
			return false;

		}
		if (isFlagFile()){ //IF dead AVD is revived
			deletelocal();
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "revive-" + myPort);
		}else { //Set a flag file as the first time this AVD was created
			CreateFlagFile();
		}


		// TODO Auto-generated method stub
		return false;
	}

	public boolean isFlagFile(){
		Context con = getContext();
		File file = new File(con.getFilesDir(), "flag");
		if(file.exists()){
			return true;
		}
		return false;
	}

	public void CreateFlagFile(){
		try {
			Context context = getContext();
			FileOutputStream outputStream;
			String filename = "flag";
			String filecontent =  "1";
			Log.e("Filecontent:", filename + "-" + filecontent);
			outputStream = context.openFileOutput(filename, context.MODE_PRIVATE);
			outputStream.write(filecontent.getBytes());
			outputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			Log.e("Exception", "at creating flag file");
			e.printStackTrace();
		}
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		SingleQueryAns = null;
		SelfSingleQueryAns = null;
		queryall = null;
		Log.e("Selection", selection);
		if (selection != null && selection.equals("@")){
			Cursor mat = localQuery();
			return  mat;
		}
		else if (selection.equals("*")){
			String data = localQueryString();
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "queryall--" + data);
			while (queryall == null){
				//Waiting for all local queries to be executed

			}
			Log.e("queryall", queryall);
			return parseGlobal(queryall);
		}

		else if (selection != null){
			String node = FindNode(selection);
			Log.e("Query", "Query:" + selection + "belongs to AVD: " + node);
			String succ1 = getSucc(node);
			String succ2 = getSucc(succ1);
			if (node.equals(myPort)){
				String answer = SingleLocalQuery(selection);
				//Get replicated keys
				new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "selfsinglequery-" + selection + "-" + succ1 + "-" + succ2 + "-" + answer) ; //key-node
				while (SelfSingleQueryAns == null){
					Log.e("Waiting", "for single Query to reach");
				}

				MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
				cursor.newRow()
						.add("key", selection)
						.add("value", answer);
				return cursor;
			}

			else {
				new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "singlequery-" + selection + "-" + node + "-" + succ1 + "-" + succ2); //key-node
				while (SingleQueryAns == null){
					//Waiting
				}
				String [] temp = SingleQueryAns.split("-");
				MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
				Log.e("Temp", temp[1]);
				cursor.newRow()
						.add("key", selection)
						.add("value", temp[1]);
				return cursor;

			}
		}
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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
    public void deletelocal(){
		Context context = getContext();
		String [] localFiles = context.fileList();
		if (localFiles.length > 0 ){
			for (int i = 0; i < localFiles.length; i++) {
					File file = new File(context.getFilesDir(), localFiles[i]);
					if (file.exists()){
						file.delete();
					}
			}
		}
	}
	public void deleteSingleFile(String filename){
		Context context = getContext();
		File file = new File(context.getFilesDir(), filename);
		if(file.exists()) {
			file.delete();
		}
	}



	public Cursor localQuery() {
		Context context = getContext();
		String ret = "";
		String[] localFiles = context.fileList();
		MatrixCursor cursor = null;
		try {
			cursor = new MatrixCursor(new String[]{"key", "value"});
			for (int i = 0; i < localFiles.length; i++) {
				if (localFiles[i].equals("flag")){
					continue;
				}
				File file = new File(context.getFilesDir(), localFiles[i]);
//				Log.e("Timestamp:", String.valueOf(file.lastModified()));
				InputStream localInput = context.openFileInput(localFiles[i]);

				InputStreamReader inputStreamReader = new InputStreamReader(localInput);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				String receiveString = "";
				StringBuilder stringBuilder = new StringBuilder();
				StringTokenizer str = new StringTokenizer(bufferedReader.readLine(), "-");
				String tp = str.nextToken();
				String version = str.nextToken();
				Log.e("Version", "key:" + tp + "::" + "version" + version);

//				while ((receiveString = tp) != null) {
//					stringBuilder.append(receiveString);
//				}
				stringBuilder.append(tp);

				localInput.close();
				ret = stringBuilder.toString();


				cursor.newRow()
						.add("key", localFiles[i])
						.add("value", ret);

				Log.v("Sel", localFiles[i]);
				Log.i("value", ret);
				//Log.i("Cursor", String.valueOf(cursor));

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return cursor;
	}

	public String localQueryString(){
		String localdata = "";
		Context context = getContext();
		String ret = "";
		String[] localFiles = context.fileList();
		MatrixCursor cursor = null;
		try {
			cursor = new MatrixCursor(new String[]{"key", "value"});
			for (int i = 0; i < localFiles.length; i++) {
				if (localFiles[i].equals("flag")){
					continue;
				}
				InputStream localInput = context.openFileInput(localFiles[i]);

				InputStreamReader inputStreamReader = new InputStreamReader(localInput);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				String receiveString = "";
				StringBuilder stringBuilder = new StringBuilder();
				StringTokenizer str = new StringTokenizer(bufferedReader.readLine(), "-");
				String tp = str.nextToken();

				String version = str.nextToken();
				Log.e("Version", "key:" + tp + "::" + "version" + version);

//				while ((receiveString = tp) != null) {
//					stringBuilder.append(receiveString);
//				}
				stringBuilder.append(tp);

				localInput.close();
				ret =  stringBuilder.toString();

				if (localdata.equals("")){
					localdata = localdata + localFiles[i] + "-" + ret;
				}else {
					localdata = localdata + "-" + localFiles[i] + "-" + ret;
				}


//				cursor.newRow()
//						.add("key", localFiles[i])
//						.add("value", ret);

				Log.v("Sel", localFiles[i]);
				Log.i("value", ret);
				//Log.i("Cursor", String.valueOf(cursor));

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return localdata;

	}

	public String SingleLocalQuery(String key) {

		Context context = getContext();
		String ret = "";
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		try {
			InputStream inputStream = context.openFileInput(key);
			InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String receiveString = "";
			StringBuilder stringBuilder = new StringBuilder();
//			StringTokenizer str = new StringTokenizer(bufferedReader.readLine(), "-");
//			String tp = str.nextToken();
//			String version = str.nextToken();
//			Log.e("Version", "key:" + tp + "::" + "version" + version);

			while ((receiveString = bufferedReader.readLine()) != null) {
				stringBuilder.append(receiveString);
			}

//			stringBuilder.append(tp);
			inputStream.close();
			ret = stringBuilder.toString();




			Log.v("Single query", key);
			Log.i("value", ret);



		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

	public Cursor parseGlobal(String gData){
		String [] d = gData.split("-");
		Log.e("len", String.valueOf(d.length));
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		for (int i = 0; i < d.length; i++){
			Log.e("i", String.valueOf(i));
			cursor.newRow()
					.add("key", d[i])
					.add("value", d[i+1]);
			i += 1;
		}

		return cursor;
	}


    public String getSucc(String nodeId){
		int n = chord.size();
		for (int i = 0; i < chord.size(); i++){
			if (chord.get(i).equals(nodeId)){
				Log.e("Successor", nodeId + "-" + chord.get((i+1)%n));
				return chord.get((i + 1) % n);
			}
		}
		return null;
	}
	public String getPred(String nodeId){
		int n = chord.size();
		for (int i = 0; i < chord.size(); i++){
			if (chord.get(i).equals(nodeId)){
				Log.e("Predecessor", nodeId + "-" + chord.get((((i - 1 % n) + n) % n)));
				return chord.get((((i - 1 % n) + n) % n));
			}
		}

		return null;
	}


    public String FindNode(String k){
		for (int i = 0; i < chord.size(); i++){
			int n = chord.size();
			String prev = chord.get((((i - 1 % n) + n) % n));
			String current = chord.get(i);
			if (CanInsert(k, prev, current)){
				Log.e("Find", "if key:" + k + "belongs to AVD:" + current + "or pred AVD:" + prev);
				return current;
			}

		}
		return null;
	}
    //Find the node where the data is to be inserted
	public boolean CanInsert(String k, String pred, String curr){
		try{
			String keyhash = genHash(k);
			String currhash = genHash(portId.get(curr));
			String predhash = genHash(portId.get(pred));

			if (keyhash.compareTo(predhash) > 0 && keyhash.compareTo(currhash) <= 0){
				return true;
			} else if (currhash.compareTo(predhash) <= 0){
				if (keyhash.compareTo(predhash) > 0 || keyhash.compareTo(currhash) <= 0){
					return true;
				}
			}else {
				return false;
			}

		}catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return false;
	}

	public String SendToInsert(String k, String v, String nodeId){
		String succ1 = getSucc(nodeId);
		String succ2 = getSucc(succ1);
		new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "insert-"+ k + "-" + v + "-" + nodeId + "-" + succ1 + "-" + succ2);

	return null;
	}
    //Method to save data on the file system
	public void InsertData(String k, String v){
		Log.e("Write to file", "key:" + k + "-" + "value:" + v);
		Context context = getContext();
		FileOutputStream outputStream;
		String filename = k;
		String filecontent = v + "-" + 1;
		Log.e("Filecontent:", filename + "-" + filecontent);
		File file = new File(context.getFilesDir(), filename);

		try {
			if (file.exists()){
				InputStream inputStream = context.openFileInput(filename);
				InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
				String br = bufferedReader.readLine(); //Get the current value as msg-version
				StringTokenizer str = new StringTokenizer(br, "-");
				String tempvalue = str.nextToken();
				String counter = str.nextToken();
				filecontent = tempvalue + "-" + (Integer.parseInt(counter)+1);
				Log.e("Counter:" , filecontent); //updated file content with new version.

				outputStream = context.openFileOutput(filename, context.MODE_PRIVATE);
				outputStream.write(filecontent.getBytes());
				outputStream.close();

			}else {
				outputStream = context.openFileOutput(filename, context.MODE_PRIVATE);
				outputStream.write(filecontent.getBytes());
				outputStream.close();
			}
		} catch (Exception e) {
			Log.e(TAG, "File write failed");
		}

	}
	public String RecoverPredData(String port){
		String localdata = "";
		Context context = getContext();
		String ret = "";
		String[] localFiles = context.fileList();
		try {

			for (int i = 0; i < localFiles.length; i++) {
				if (FindNode(localFiles[i]).equals(port)) { //If this is my own key, send it back to recovered AVD for replication.
					if (localFiles[i].equals("flag")){
						continue;
					}
					InputStream localInput = context.openFileInput(localFiles[i]);

					InputStreamReader inputStreamReader = new InputStreamReader(localInput);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String receiveString = "";
					StringBuilder stringBuilder = new StringBuilder();

					while ((receiveString = bufferedReader.readLine()) != null) {
						stringBuilder.append(receiveString);
					}

					localInput.close();
					ret = stringBuilder.toString();
					Log.e("PSdata", ret);

					if (localdata.equals("")) {
						localdata = localdata + localFiles[i] + "--" + ret;
					} else {
						localdata = localdata + "--" + localFiles[i] + "--" + ret;
					}


//					Log.v("Key", localFiles[i]);
//					Log.i("value", ret);
					//Log.i("Cursor", String.valueOf(cursor));

				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return localdata;

	}
	public void SaveRecoveredData(String recover_data){
		Log.e("SaveRecover:", recover_data);
		String [] rec_data = recover_data.split("--");
		Log.e("Len", String.valueOf(rec_data.length));
		FileOutputStream outputStream;
		Context context = getContext();
		for (int i = 0; i<rec_data.length; i++){
			Log.e("i", String.valueOf(i));
			String filename = rec_data[i];
			String filecontent = rec_data[i+1];
//			String [] final_counter = filecontent.split("-");
			Log.e("Recover", "recover key:" + filename + "|" + "recover value:" + filecontent);

			File file = new File(context.getFilesDir(), filename);


			try {
				outputStream = context.openFileOutput(filename, context.MODE_PRIVATE);
				outputStream.write(filecontent.getBytes());
				outputStream.close();
			} catch (Exception e) {
				Log.e(TAG, "File write failed");
			}

			i = i + 1;
		}

	}
	public String FindMax(String all_data){
		String [] query_ans = all_data.split("--");
		Log.e("Query_ans", String.valueOf(query_ans));
		StringTokenizer str = new StringTokenizer(query_ans[0], "-");
		String final_value = str.nextToken();
		String currmax = str.nextToken();
		for (int i = 1; i < query_ans.length; i++){
			StringTokenizer currStr = new StringTokenizer(query_ans[i], "-");
			String currString = currStr.nextToken();
			String currCount = currStr.nextToken();

			if (Integer.parseInt(currCount) > Integer.parseInt(currmax)){
				currmax = currCount;
				final_value = currString;
			}
		}
		Log.e("Max value", final_value);

		return final_value;
	}




	private class ClientTask extends AsyncTask<String, Void, Void>{
		ArrayList<String> tempList = new ArrayList<String>();
		@Override
		protected Void doInBackground(String... requests) {
			String request = requests[0];
			Log.e("request", request);

			//Insert Request
			if (request.startsWith("insert")){
				String [] InsertData = request.split("-");
				tempList.add(InsertData[3]);
				tempList.add(InsertData[4]);
				tempList.add(InsertData[5]);


				for (int i = 0; i < tempList.size(); i++){
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(tempList.get(i)));
						socket.setSoTimeout(2000);
						PrintWriter pf0 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						Log.e("Sent", "Key-"+ InsertData[1] + "sent to- " + tempList.get(i));
						pf0.println("insert-" + InsertData[1] + "-" +InsertData[2]); //Key-Value
						pf0.flush();

						String ack = br.readLine();
						Log.e("Receive", "Ack back :" + ack);

						if (ack != null && ack.equals("ack")){
							socket.close();
						} else throw new IOException();
						} catch (IOException e) {

							Log.e("Exception", "Insert failed. AVD dead:");
							e.printStackTrace();
						}
				}
				tempList.clear();
			} else if (request != null && request.startsWith("singlequery")){
				ArrayList<String> SQList = new ArrayList<String>();
				Socket socket = null;
				String [] singleQuery = request.split("-");
				SQList.add(singleQuery[2]);
				SQList.add(singleQuery[3]);
				SQList.add(singleQuery[4]);
				String data = ""; //
				for (int i = 0; i < SQList.size(); i++) {
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SQList.get(i)));
						socket.setSoTimeout(2000);
						PrintWriter pf1 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
						BufferedReader br1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						Log.e("Single Q", "Single Query Request sent");
						pf1.println("singlequery-" + singleQuery[1]); //Key
						pf1.flush();

						String ack1 = br1.readLine();
						if (ack1 != null ){
							socket.close();
							if (data != null){
								data = data + "--" + ack1;
							}else {
								data = ack1;
							}

						} else throw new IOException();
					} catch (IOException e) {

						e.printStackTrace();
						Log.e("Exception", "Single query failed");
					}
				}
				SingleQueryAns = FindMax(data);
				SQList.clear();
			}
			else if (request != null && request.startsWith("selfsinglequery")){
				ArrayList<String> SQList2 = new ArrayList<String>();
				Socket socket = null;
				String [] singleQuery2 = request.split("-");
				SQList2.add(singleQuery2[2]);
				SQList2.add(singleQuery2[3]);
				String data =  singleQuery2[4];
				for (int i = 0; i < SQList2.size(); i++) {
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(SQList2.get(i)));
						socket.setSoTimeout(2000);
						PrintWriter pf1 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
						BufferedReader br2 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						Log.e("Self Single Q", "Single Query Request sent");
						pf1.println("selfsinglequery-" + singleQuery2[1]); //Key
						pf1.flush();

						String rr = br2.readLine();

						if (rr != null){
							if (data != null){
								data = data + "--" + rr;
							} else {
								data = rr;
							}
						} else throw new IOException();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				SelfSingleQueryAns = FindMax(data);
				SQList2.clear();
			} else if (request!=null && request.startsWith("queryall")){
				Socket socket = null;
				String [] QA = request.split("--");
				String queryall_data = QA[1];
				for (int i = 0; i < chord.size(); i++){

					try {
						if (chord.get(i).equals(myPort)){
							continue;
						}
						Log.e("QA", "Forwarding query all to AVD:" + chord.get(i));
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chord.get(i)));
						socket.setSoTimeout(2000);
						PrintWriter pf2 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
						BufferedReader br3 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						Log.e("Query all", "Query All Request sent");
						pf2.println("queryall"); //Key
						pf2.flush();

						String rr2 = br3.readLine();
						if (rr2 != null){
							socket.close();
							queryall_data = queryall_data + "-" + rr2;

						} else throw new IOException();

					}  catch (IOException e) {

						Log.e("Exception", "Query all failed");
						e.printStackTrace();
					}
				}
				queryall = queryall_data;
			}
			else if (request != null && request.startsWith("DeleteAll")){
				Socket socket3 = null;
				for (int i = 0; i < chord.size(); i++){
					try {
						if (chord.get(i).equals(myPort)){
							continue;
						}
						socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chord.get(i)));
						socket3.setSoTimeout(2000);
						PrintWriter pf3 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream())));
//						BufferedReader br3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
						Log.e("Query all", "Query All Request sent");
						pf3.println("deleteAll"); //Key
						pf3.flush();
					}  catch (IOException e) {

						Log.e("exception", "delete all");
						e.printStackTrace();
					}
				}
			}
			else if (request != null && request.startsWith("deleteSingle")){
				Socket socket4 = null;
				String [] delSingle = request.split("-");
				String dest = delSingle[1];
				String succ1 = getSucc(dest);
				String succ2 = getSucc(succ1);
				String [] AVD_del = {dest, succ1, succ2};
				for (int i = 0; i < AVD_del.length; i++){
					try {

						socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(AVD_del[i]));
						socket4.setSoTimeout(2000);
						PrintWriter pf4 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket4.getOutputStream())));
//						BufferedReader br3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
						Log.e("DEL single", "Delete single sent");
						pf4.println( "Delete-" + delSingle[2]); //Key
						pf4.flush();
					}  catch (IOException e) {

						Log.e("exception", "delete single");
						e.printStackTrace();
					}
				}
			} else if (request!=null && request.startsWith("revive")){ //Dead AVD is revived
				//Get replica copies back from pred and get my data from succ
				Log.e("Recover AVD:", myPort);
				String [] replicate = request.split("-");
				String succ1 = getSucc(replicate[1]);
				String succ2 = getSucc(succ1);
				String pred1 = getPred(replicate[1]);
				String pred2 = getPred(pred1);
				String [] dead_pred = {pred1, pred2};
				String [] dead_succ = {succ1, succ2};
				Socket socket5 = null;
				String pred_data = "";
				for (int i = 0; i < dead_pred.length; i++){
					try{
						socket5 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(dead_pred[i]));
						socket5.setSoTimeout(2000);
						PrintWriter pf5 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket5.getOutputStream())));
						BufferedReader br5 = new BufferedReader(new InputStreamReader(socket5.getInputStream()));
						Log.e("recover predecessor", dead_pred[i]);
						pf5.println("Pred");
						pf5.flush();
						String predbr = br5.readLine();

						if (predbr != null){
							socket5.close();
							Log.e("Pred data", predbr);
							SaveRecoveredData(predbr);

//							if (pred_data.equals("")){
//							pred_data = pred_data  + predbr;
//						} else {
//								pred_data = pred_data + "--" + predbr;
//							}
						}
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				//Now save recovered data from predecessors
				;
				Log.e("Done", "Recovered from Preds");
				String succ_data = "";
				//Same for Successors
				for (int i = 0; i < dead_succ.length; i++){
					try{
						socket5 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(dead_succ[i]));
						socket5.setSoTimeout(2000);
						PrintWriter pf5 = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket5.getOutputStream())));
						BufferedReader br5 = new BufferedReader(new InputStreamReader(socket5.getInputStream()));
						Log.e("recover succ", dead_succ[i]);
						pf5.println("Succ-" + myPort);
						pf5.flush();
						String succbr = br5.readLine();

						if (succbr != null){
							socket5.close();
							Log.e("Succ data", succbr);
							SaveRecoveredData(succbr);
//							if (succ_data.equals("")){
//								succ_data = succ_data  + succbr;
//							} else {
//								succ_data = succ_data + "--" + succbr;
//							}
						}
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				//Now save recovered data from successors

//				SaveRecoveredData(succ_data);


			}
			return null;
		}
	}
	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serversocket = serverSockets[0];
			Socket s = null;
			try{
				while (true){
					s = serversocket.accept();
					BufferedReader br1 = new BufferedReader(new InputStreamReader(s.getInputStream()));
					PrintWriter pp = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
					String req = br1.readLine();
					Log.e("Server", req);

					//Handle Insert request at Server
					if (req != null && req.startsWith("insert")){
						String [] insert = req.split("-");
						InsertData(insert[1], insert[2]); //key-value
						pp.println("ack");
						pp.flush();
					}
					else if (req != null && req.startsWith("singlequery")){
						String [] SQ = req.split("-");
						String temp = SingleLocalQuery(SQ[1]);
						pp.println(temp);
						pp.flush();

					}
					else if (req != null && req.startsWith("selfsinglequery")){
						s.close();
						String [] SQ = req.split("-");
						String temp1 = SingleLocalQuery(SQ[1]);
						pp.println(temp1);
						pp.flush();
					}
					else if (req!= null && req.equals("queryall")){
						String tempdata = localQueryString();
						pp.println(tempdata);
						pp.flush();
					}
					else if (req != null && req.equals("DeleteAll")){
						s.close();
						deletelocal();
					}
					else if (req != null && req.startsWith("Delete")){
						String [] del = req.split("-");
						s.close();
						deleteSingleFile(del[1]);
					}
					else if (req != null && req.equals("Pred")){
						String recover_data =  RecoverPredData(myPort);
						pp.println(recover_data);
						pp.flush();
					}
					else if (req != null && req.startsWith("Succ")){
						Log.e("Succ", "in suuccc.");
						String[] suc = req.split("-");
						String suc_data = RecoverPredData(suc[1]);
						Log.e("suc_data", suc_data);
						pp.println(suc_data);
						pp.flush();
					}

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}
}
