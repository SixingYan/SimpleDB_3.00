import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class InsertBusinessLocation {
	
	public static void main(String[] args) {
		try {
			SimpleDB.init("cs542");
			Transaction tx = new Transaction();
			// 1. init index file
			SimpleBuildRTree SBR = new SimpleBuildRTree();
			SBR.BuildRTree("businesslocation", "latitude", "longitude");
			
			// insert data
			File file = new File("insertdata.txt");
			BufferedReader bfreader = null;
			String content = null;
			try {
				FileReader reader = new FileReader(file);

				bfreader = new BufferedReader(reader);
				content = bfreader.readLine();
				while(content!=null) {
					SBR.insert(content);
					System.out.println("1 RTest records inserted.");
					content = bfreader.readLine();
				}

			} catch (IOException h) {
				// TODO Auto-generated catch block
				h.printStackTrace();
			} finally {
				try {
					bfreader.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

}
