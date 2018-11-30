package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.query.Predicate;
/**
 * The SimpleDB parser.
 * @author Edward Sciore | Sixing Yan
 */
public class Parser {
	private Lexer lex;
	public static String DFLT_OPRT = "=";

	public Parser(String s) {
		lex = new Lexer(s);
	}
	
	
	public static void main(String args[]) {
		String sql = "select max(a), b from b where distance() = a";
		QueryData qd = new Parser(sql).query();
		
	}
	
// Methods for parsing predicates, terms, expressions, constants, and fields
	
	/**
	 * 
	 * @return
	 */
	public String field() {
		if (lex.matchAggFn()) { // aggfn(fld)
			String fn = lex.eatAggFn();
			lex.eatDelim(); // '('
			String fld = lex.eatId();
			lex.eatDelim(); // ')'
			return fn + "(" + fld + ")";
		} else if (lex.matchFn()) { // fn()
			String fn = lex.eatFn();
			lex.eatDelim(); // '('
			lex.eatDelim(); // ')'
			return fn + "()";
		}
		else
			return lex.eatId();
	}

	public Constant constant() {
		if (lex.matchStringConstant())
			return new StringConstant(lex.eatStringConstant());
		else if (lex.matchFloatConstant())
			return new FloatConstant(lex.eatFloatConstant());
		else
			return new IntConstant(lex.eatIntConstant());
	}

	public String operator() {
		if (lex.matchOperator())
			return lex.eatOperator();
		lex.eatDelim();
		return DFLT_OPRT;
	}

	/**
	 *
	 * @return
	 */
	public Expression expression() {

		if (lex.matchFn())
			return new FieldNameExpression(field());
		else {
			if (lex.matchId())
				return new FieldNameExpression(field());
			else
				return new ConstantExpression(constant());
		}
	}

	public Term term() {
		Expression lhs = expression();
		if (lhs != null) {
			String opt = operator();
			Expression rhs = expression();
			return new Term(lhs, rhs, opt);
		}
		return null;
	}
	/**
	 *
	 * @return
	 */
	public Predicate predicate() {
		Predicate pred = null;
		Term t = term();
		if (t != null) {
			pred = new Predicate(t);
			if (lex.matchKeyword("and")) {
				lex.eatKeyword("and");
				pred.conjoinWith(predicate());
			}
		}
		return pred;
	}

// Methods for parsing queries

	/**
	 *
	 * @return
	 */
	public QueryData query() {
		// 1. Basic elements of a query
		lex.eatKeyword("select");
		Collection<String> rawfields = selectList();
		Collection<String> fields = filedList(rawfields);
		Collection<String> aggfns = aggfnsList(rawfields);

		lex.eatKeyword("from");
		Collection<String> tables = tableList();

		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}

		// 2. create a query data
		QueryData qd = new QueryData(fields, tables, pred);

		// 3. group by
		if (lex.matchKeyword("groupby")) {
			lex.eatKeyword("groupby");
			Collection<String> groupflds = groupby();
			qd.setGroupByFields(groupflds);
			qd.setAggfns(aggfns);
		}
		return qd;
	}

	/**
	 *
	 * @return
	 */
	private Collection<String> groupby() {
		Collection<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

	/**
	 *
	 * @param rawfields
	 * @return
	 */
	private Collection<String> filedList(Collection<String> rawfields) {
		Collection<String> L = new ArrayList<String>();
		for (String t : rawfields)
			if (!t.contains("("))
				L.add(t);
		return L;
	}

	/**
	 *
	 * @param rawfields
	 * @return
	 */
	private Collection<String> aggfnsList(Collection<String> rawfields) {
		Collection<String> L = new ArrayList<String>();
		for (String t : rawfields)
			if (t.contains("("))
				L.add(t);
		return L;
	}

	/**
	 *
	 * @return
	 */
	private Collection<String> selectList() {
		Collection<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

	private Collection<String> tableList() {
		Collection<String> L = new ArrayList<String>();
		L.add(lex.eatId());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(tableList());
		}
		return L;
	}

// Methods for parsing the various update commands

	public Object updateCmd() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else
			return create();
	}

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("table"))
			return createTable();
		else if (lex.matchKeyword("view"))
			return createView();
		else
			return createIndex();
	}

// Method for parsing delete commands

	public DeleteData delete() {
		lex.eatKeyword("delete");
		lex.eatKeyword("from");
		String tblname = lex.eatId();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new DeleteData(tblname, pred);
	}

// Methods for parsing insert commands

	public InsertData insert() {
		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		List<String> flds = fieldList();
		lex.eatDelim(')');
		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(tblname, flds, vals);
	}

	private List<String> fieldList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(fieldList());
		}
		return L;
	}

	private List<Constant> constList() {
		List<Constant> L = new ArrayList<Constant>();
		L.add(constant());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(constList());
		}
		return L;
	}

// Method for parsing modify commands

	public ModifyData modify() {
		lex.eatKeyword("update");
		String tblname = lex.eatId();
		lex.eatKeyword("set");
		String fldname = field();
		lex.eatDelim('=');
		Expression newval = expression();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new ModifyData(tblname, fldname, newval, pred);
	}

// Method for parsing create table commands

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema fieldDef() {
		String fldname = field();
		return fieldType(fldname);
	}

	/** @author Sixing Yan
	 * Returns true if both of the term's expressions
	 * evaluate to the same constant,
	 * Only work with 'primary key'
	 * @param fldname the scan
	 * @return true if both expressions have the same value in the scan
	*/
	private Schema fieldType(String fldname) {
		Schema schema = new Schema();
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addIntField(fldname);
		} else if (lex.matchKeyword("varchar")) {
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen);
		} else {
			lex.eatKeyword("float");
			schema.addFloatField(fldname);
		}
		return schema;
	}

// Method for parsing create view commands

	public CreateTableData createTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		Schema sch = fieldDefs();
		lex.eatDelim(')');
		return new CreateTableData(tblname, sch);
	}

	public CreateViewData createView() {
		lex.eatKeyword("view");
		String viewname = lex.eatId();
		lex.eatKeyword("as");
		QueryData qd = query();
		return new CreateViewData(viewname, qd);
	}


//  Method for parsing create index commands

	public CreateIndexData createIndex() {
		lex.eatKeyword("index");
		String idxname = lex.eatId();
		lex.eatKeyword("on");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		String fldname = field();
		lex.eatDelim(')');
		return new CreateIndexData(idxname, tblname, fldname);
	}
}

