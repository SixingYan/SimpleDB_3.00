import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
*
* @author Xinmeng Gu | Sixing Yan
*/
public class CreateBusinessLocation {
    public static void main(String[] args) {
		try {
			SimpleDB.init("dbdata");
			Transaction tx = new Transaction();
			// 1. create table
			String s = "create table businesslocation(blid varchar(100),city varchar(45),state varchar(45),"
					+ "postalcode varchar(45),address varchar(255),"
					+ "latitude float,longitude float)";
			
			SimpleDB.planner().executeUpdate(s, tx);
			System.out.println("Table businesslocation created.");			
			tx.commit();
			
			// 2. create index info
			s = "";
			SimpleDB.planner().executeUpdate(s, tx);
			System.out.println("index businesslocationrt created.");
			
			// 3. create index file
			SimpleBuildRTree SBR=new SimpleBuildRTree();
			SBR.BuildRTree("businesslocation", "latitude", "longitude");

		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
