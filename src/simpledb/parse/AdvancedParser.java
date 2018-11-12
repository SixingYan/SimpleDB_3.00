public class Parser {
	private Lexer lex;
   
	public Parser(String s) {
		lex = new Lexer(s);
	}

	public String operator() {
		if (lex.matchOperator())
			return 
	}
}