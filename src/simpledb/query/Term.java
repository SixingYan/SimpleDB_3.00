package simpledb.query;

import simpledb.record.Schema;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore | Sixing Yan
 *
 */
public class Term {
    private Expression lhs, rhs;
    private String operator;
    /**
     * Creates a new term that compares two expressions
     * for equality.
     * @param lhs  the LHS expression
     * @param rhs  the RHS expression
     */
    public Term(Expression lhs, Expression rhs, String operator) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.operator = operator;
    }

    /**
     * Calculates the extent to which selecting on the term reduces
     * the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * term cuts the size of the output in half.
     * @param p the query's plan
     * @return the integer reduction factor.
     */
    public int reductionFactor(Plan p) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(p.distinctValues(lhsName),
                            p.distinctValues(rhsName));
        }
        if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return p.distinctValues(lhsName);
        }
        if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return p.distinctValues(rhsName);
        }
        // otherwise, the term equates constants
        if (lhs.asConstant().equals(rhs.asConstant()))
            return 1;
        else
            return Integer.MAX_VALUE;
    }


    /**
     * Determines if this term is of the form "F (*) c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     * @param fldname the name of the field
     * @return either the constant or null
     */
    public Constant operatesWithConstant(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                rhs.isConstant())
            return rhs.asConstant();
        else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 lhs.isConstant())
            return lhs.asConstant();
        else
            return null;
    }

    /**
     * Determines if this term is of the form "F1 (*) F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     * @param fldname the name of the field
     * @return either the name of the other field, or null
     */
    public String operatesWithField(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                rhs.isFieldName())
            return rhs.asFieldName();
        else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 lhs.isFieldName())
            return lhs.asFieldName();
        else
            return null;
    }

    /**
     * Determines if this term is of the form "F (*) c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     * @param fldname the name of the field
     * @return either the constant or null
     */
    public Constant equatesWithConstant(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                rhs.isConstant())
            return rhs.asConstant();
        else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 lhs.isConstant())
            return lhs.asConstant();
        else
            return null;
    }

    /**
     * Determines if this term is of the form "F1 (*) F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     * @param fldname the name of the field
     * @return either the name of the other field, or null
     */
    public String equatesWithField(String fldname) {
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                rhs.isFieldName())
            return rhs.asFieldName();
        else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 lhs.isFieldName())
            return lhs.asFieldName();
        else
            return null;
    }

    /**
     * Returns true if both of the term's expressions
     * apply to the specified schema.
     * @param sch the schema
     * @return true if both expressions apply to the schema
     */
    public boolean appliesTo(Schema sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
    }

    /** @author Sixing Yan
     * Returns true if both of the term's expressions
     * evaluate to the same constant,
     * with respect to the specified scan.
     * @param s the scan
     * @return true if both expressions have the same value in the scan
    */
    public boolean isSatisfied(Scan s) {
        Constant lhsval = lhs.evaluate(s);
        Constant rhsval = rhs.evaluate(s);
        return isSatisfiedOperator(lhsval, rhsval);
    }

    public String toString() {
        return lhs.toString() + this.operator + rhs.toString();
    }

    public Boolean isSatisfiedOperator(Constant lhsval, Constant rhsval) {
        if (this.operator == "like") return isLikeSatisfied(lhsval, rhsval);

        int compare = lhsval.compareTo(rhsval);
        if (this.operator == ">" & compare > 0) return true;
        if (this.operator == ">=" & compare >= 0) return true;
        if (this.operator == "<" & compare < 0) return true;
        if (this.operator == "<=" & compare <= 0) return true;
        if (this.operator == "=" & compare == 0) return true;
        if (this.operator == "<>" & compare != 0) return true;
        return false;
    }

    /** @author Sixing Yan
     * Returns true if both of the term's expressions
     * evaluate to the same constant,
     * Only work with 'A%'
     * @param s the scan
     * @return true if both expressions have the same value in the scan
    */
    public Boolean isLikeSatisfied(Constant lhsval, Constant rhsval) {
        String lval = (String) lhsval.asJavaVal();
        String rval = (String) rhsval.asJavaVal();
        String keyword = rval.split("%")[0];
        if (keyword.startsWith(lval))
            return true;
        return false;
    }
}
