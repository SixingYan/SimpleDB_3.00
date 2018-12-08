import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.query.Plan;
import simpledb.query.StringConstant;
import simpledb.record.TableInfo;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
/**
 * 
 * @author Xinmeng Gu
 */
public class SimpleRTreeTest {
    public static void main(String[] args) {
		Connection conn = null;
		try {
			SimpleDB.init("dbdata");
			Transaction tx = new Transaction();
			
			String s = "create table RTest (rname varchar(10), x float, y float)";
			SimpleDB.planner().executeUpdate(s, tx);
			System.out.println("Table RTest created.");
			
			tx.commit();
			
			SimpleBuildRTree SBR=new SimpleBuildRTree();
			
			SBR.BuildRTree("RTest", "x", "y");
			
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
