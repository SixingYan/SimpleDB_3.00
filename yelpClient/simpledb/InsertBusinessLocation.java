import java.sql.*;
import simpledb.remote.SimpleDriver;

public class InsertBusinessLocation {
	public static void main(String[] args) {
		Connection conn = null;
		try {
			// 1. init index file
			SimpleBuildRTree SBR = new SimpleBuildRTree();

			SBR.BuildRTree("businesslocation", "latitude", "longitude");

			// 2. insert data
			String s2 = "insert into RTest(rname, x, y) values ";
			String[] studvals = {"('a', 3.0, 3.0)",
			                     "('b', 4.0, 4.0)",
			                     "('c', 5.0, 5.0)",
			                     "('d', 6.0, 6.0)",
			                     "('e', 7.0, 7.0)",
			                    };

			for (int i = 0; i < studvals.length; i++) {
				SBR.insert(s2 + studvals[i]);
				System.out.println("1 RTest records inserted.");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
