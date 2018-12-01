import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateBusinessLocation {
    public static void main(String[] args) {
		try {
			SimpleDB.init("dbdata");
			Transaction tx = new Transaction();
			
			String s = "create table businesslocation(blid varchar(100),city varchar(45),state varchar(45),"
					+ "postalcode varchar(45),address varchar(255),"
					+ "latitude float,longitude float)";
			
			SimpleDB.planner().executeUpdate(s, tx);
			System.out.println("Table RTest created.");
			
			tx.commit();
			
			SimpleBuildRTree SBR=new SimpleBuildRTree();
			
			SBR.BuildRTree("businesslocation", "latitude", "longitude");
			
			String s2 = "insert into RTest(rname, x, y) values ";
			String[] studvals = {"('a', 3.0, 3.0)",
					             "('b', 4.0, 4.0)",
					             "('c', 5.0, 5.0)",
					             "('d', 6.0, 6.0)",
					             "('e', 7.0, 7.0)",};

			for (int i=0; i<studvals.length; i++)
			{
				SBR.insert(s2 + studvals[i]);
				System.out.println("1 RTest records inserted.");
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
