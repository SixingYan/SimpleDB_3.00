package simpledb.parse;

import java.util.*;
import simpledb.query.*;
import simpledb.record.Schema;

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

// Methods for parsing predicates, terms, expressions, constants, and fields

	public String field() {
		return lex.eatId();
	}

	public Constant constant() {
		if (lex.matchStringConstant())
			return new StringConstant(lex.eatStringConstant());
		else
			return new IntConstant(lex.eatIntConstant());
	}

	public String operator() {
		if (lex.matchOperator())
			return lex.eatOperator();
		return DFLT_OPRT;
	}

	public Expression expression() {
		if (lex.matchId())
			return new FieldNameExpression(field());
		else if ()
			return new ConstantExpression(constant());
		else
			return null;
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

	public Predicate predicate() {
		Term t = term();
		if t != null
		Predicate pred = new Predicate(term());
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjoinWith(predicate());
		}
		return pred;
	}


	private boolean matchFn (String w) {
		if (w.contains("("))
			return lex.fns().contains(w.split("(")[0]);
		else
			return false;
	}

// Methods for parsing queries

	public QueryData query() {
		// Basic elements of a query
		lex.eatKeyword("select");
		Collection<String> rawfields = selectList();
		Collection<String> fields = filedList();
		Collection<String> aggfns = aggfnsList();

		lex.eatKeyword("from");
		Collection<String> tables = tableList();

		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}

		// group by
		Collection<String> groupflds;
		if (lex.matchKeyword("groupby")) {
			lex.eatKeyword("groupby");
			groupflds = groupbyList();
		}

		// not groupby but aggfns, move aggfns to projectplan
		if (groupflds == null & !aggfns.isEmpty()) {
			fields.addAll(aggfns);
			aggfns.clean();
		}

		QueryData qd = new QueryData(fields, tables, pred);

		// it will create the parameters of groupbyplan
		if (groupflds != null) {
			qd.setGroupByFields(groupflds);
			qd.setAggfns(aggfns);
		}

		return qd;
	}

	private Collection<String> groupby() {
		Collection<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

	private Collection<String> filedList(Collection<String> rawfields) {
		// 判断哪些是直接的field
		Collection<String> L = new ArrayList<String>();


		return L
	}

	private Collection<String> aggfnsList(Collection<String> fields) {
		// 这里先不要实例化函数，直给出完整的形式，max(id)
		Collection<String> L = new ArrayList<String>();

		Iterator<String> it = fields.iterator();
		while (it.hasNext())
			if (matchFn(it.next()))
				L.add(it.next());
		return L
	}

	private Collection<String> selectList() {
		// 这里还要加入 distance(x,y) 类型的判断，不能遇到 “,” 就分割
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

