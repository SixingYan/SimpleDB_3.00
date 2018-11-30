package simpledb.parse;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * @author Edward Sciore | Sixing Yan
 */
public class Lexer {
    private Collection<String> keywords;
    private Collection<String> operators;
    private StreamTokenizer tok;
    private Collection<String> functions;
    private Collection<String> aggFunctions;
    public static String FN_SIGN = "(";
    /**
     * Creates a new lexical analyzer for SQL statement s.
     * @param s the SQL statement
     */
    
    public static void main(String args[]) {
    	String sql = "select max(a), b.b from b where dis() = a";
    	Lexer lex = new Lexer(sql);
    	for (int i=0; i< 15; i++)
    		lex.print();
    } 
    
    public Lexer(String s) {
        initKeywords();
        initOperators();
        initFunctions();
        tok = new StreamTokenizer(new StringReader(s));
        tok.ordinaryChar('.');
        tok.wordChars(40, 41); // add ascii range to support () 
        tok.wordChars(60, 62); // add ascii range to support < = >
        tok.lowerCaseMode(true); //ids and keywords are converted
        nextToken();
    }
    
    public void print() {
    	// only for testing
    	System.out.println(tok.sval);
    	nextToken();
    }

    /**
     * 
     * @return
     */
    public boolean matchFn() {
    	return tok.ttype == StreamTokenizer.TT_WORD && functions.contains(tok.sval);
    } 
    
    /**
     * 
     * @return
     */
    public boolean matchAggFn() {
    	return tok.ttype == StreamTokenizer.TT_WORD && aggFunctions.contains(tok.sval);
    }
    
    /**
     * Returns true if the current token is
     * the specified delimiter character.
     * @param d a character denoting the delimiter
     * @return true if the delimiter is the current token
     */
    public boolean matchDelim(char d) {
        return d == (char)tok.ttype;
    }

    /**
     * Returns true if the current token is an integer.
     * @return true if the current token is an integer
     */
    public boolean matchIntConstant() {
        return tok.ttype == StreamTokenizer.TT_NUMBER;
    }

    /**
     * Returns true if the current token is a string.
     * @return true if the current token is a string
     */
    public boolean matchStringConstant() {
        return '\'' == (char)tok.ttype;
    }

    /**@author Sixing Yan
     * Returns true if the current token is a number.
     * @return true if the current token is a number
     */
    public boolean matchFloatConstant() {
        return tok.ttype == StreamTokenizer.TT_NUMBER;
    }

    /**
     * Returns true if the current token is the specified keyword.
     * @param w the keyword string
     * @return true if that keyword is the current token
     */
    public boolean matchKeyword(String w) {
        return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
    }

    /**
     * Returns true if the current token is the specified keyword.
     * @return true if that keyword is the current token
     */
    public boolean matchKeyword() {
        return tok.ttype == StreamTokenizer.TT_WORD && keywords.contains(tok.sval);
    }
    
    public boolean matchFunction() {
        return tok.sval.contains(FN_SIGN);
    }

    /**
     * Returns true if the current token is a legal identifier.
     * @return true if the current token is an identifier
     */
    public boolean matchId() {
        return  tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
    }

    public boolean matchOperator() {
        return operators.contains(tok.sval);
    }

//Methods to "eat" the current token
    
    public String eatFn() {
    	if (!matchFn())
            throw new BadSyntaxException();
    	String s = tok.sval;
    	System.out.println("now eat fn " + tok.sval);
        nextToken();
        return s;
    }
    
    public String eatAggFn() {
    	if (!matchAggFn())
            throw new BadSyntaxException();
    	String s = tok.sval;
    	System.out.println("now eat agg " + tok.sval);
        nextToken();
        return s;
    }
    
    /**
     * Throws an exception if the current token is not the
     * specified delimiter.
     * Otherwise, moves to the next token.
     * @param d a character denoting the delimiter
     */
    public void eatDelim(char d) {
        if (!matchDelim(d))
            throw new BadSyntaxException();
        System.out.println("now eat delim " + tok.sval);
        nextToken();
    }
    
    /**
     * 
     */
    public void eatDelim() {
    	System.out.println("now eat delim " + tok.sval);
    	nextToken();
    }
    
    /**
     * Throws an exception if the current token is not
     * an integer.
     * Otherwise, returns that integer and moves to the next token.
     * @return the integer value of the current token
     */
    public int eatIntConstant() {
        if (!matchIntConstant()) {
        	System.out.println(tok.sval);
        	System.out.println(tok.nval);
            throw new BadSyntaxException();
        }
        int i = (int) tok.nval;
        System.out.println("now eat int " + tok.sval);
        nextToken();
        return i;
    }

    /**
     * Throws an exception if the current token is not
     * a string.
     * Otherwise, returns that string and moves to the next token.
     * @return the string value of the current token
     */
    public String eatStringConstant() {
        if (!matchStringConstant())
            throw new BadSyntaxException();
        String s = tok.sval; //constants are not converted to lower case
        System.out.println("now eat string " + tok.sval);
        nextToken();
        return s;
    }

    /**@author Sixing Yan
     * Throws an exception if the current token is not
     * a float.
     * Otherwise, returns that string and moves to the next token.
     * @return the number value of the current token
     */
    public float eatFloatConstant() {
        if (!matchFloatConstant())
            throw new BadSyntaxException();
        float i = new Float(tok.nval);
        System.out.println("now eat float " + tok.sval);
        nextToken();
        return i;
    }
    
    /**
     * Throws an exception if the current token is not the
     * specified keyword.
     * Otherwise, moves to the next token.
     * @param w the keyword string
     */
    public void eatKeyword(String w) {
        if (!matchKeyword(w))
            throw new BadSyntaxException();
        System.out.println("now eat keyword " + tok.sval);
        nextToken();
    }

    /**
     * Throws an exception if the current token is not
     * an identifier.
     * Otherwise, returns the identifier string
     * and moves to the next token.
     * @return the string value of the current token
     */
    public String eatId() {
        if (!matchId())
            throw new BadSyntaxException();
        String s = tok.sval;
        System.out.println("now eat id " + tok.sval);
        nextToken();
        return s;
    }

    public String eatOperator() {
        if (!matchOperator())
            throw new BadSyntaxException();
        String s = tok.sval;
        System.out.println("now eat operator " + tok.sval);
        nextToken();
        return s; 
    }

    private void nextToken() {
        try {
            tok.nextToken();
        } catch (IOException e) {
            throw new BadSyntaxException();
        }
    }

    private void initKeywords() {
        this.keywords = Arrays.asList("select", "from", "where", "and",
                                 "insert", "into", "values", "delete", "update", "set",
                                 "create", "table", "int", "varchar", "float", "view", "as", "index", "on", "distance");
    }

    /** @author Sixing Yan
     * Throws an exception if the current token is not the
     * specified keyword.
     * Otherwise, moves to the next token.
     * @param w the keyword string
     */
    private void initOperators() {
        this.operators = Arrays.asList(">", "<", "<>", ">=", "<=", "=", "like");
    }


    private void initFunctions() {
    	this.aggFunctions = Arrays.asList("count", "max");
        this.functions = Arrays.asList("distance");
    }
    
    public Collection<String> fns() {
        return this.functions;
    }
    
    public Collection<String> aggfns() {
        return this.aggFunctions;
    } 
}
