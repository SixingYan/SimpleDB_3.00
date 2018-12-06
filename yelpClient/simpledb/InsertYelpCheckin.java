import java.sql.*;
import simpledb.remote.SimpleDriver;

public class InsertYelpCheckin {
	// 
    public static void main(String[] args) {
		Connection conn = null;
		try {
			// need to edit anyway
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
			String[] studvals = {"(1, 'joe', 10, 2004)",
								 "(2, 'amy', 20, 2004)",
								 "(3, 'max', 10, 2005)",
								 "(4, 'sue', 20, 2005)",
								 "(5, 'bob', 30, 2003)",
								 "(6, 'kim', 20, 2001)",
								 "(7, 'art', 30, 2004)",
								 "(8, 'pat', 20, 2001)",
								 "(9, 'lee', 10, 2004)"};
			for (int i=0; i<studvals.length; i++)
				stmt.executeUpdate(s + studvals[i]);
			System.out.println("STUDENT records inserted.");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
