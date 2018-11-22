package simpledb.query;

/**
 * The class that wraps Java ints as database constants.
 * @author Sixing Yan
 */
public class FloatConstant implements Constant {
   private Float val;
   
   /**
    * Create a constant by wrapping the specified int.
    * @param n the float value
    */
   public FloatConstant(float n) {
      val = n;
   }
   
   /**
    * Unwraps the Float and returns it.
    * @see simpledb.query.Constant#asJavaVal()
    */
   public Float asJavaVal() {
      return val;
   }
   
   public boolean equals(Object obj) {
      FloatConstant ic = (FloatConstant) obj;
      return ic != null && val.equals(ic.val);
   }
   
   public int compareTo(Constant c) {
      FloatConstant ic = (FloatConstant) c;
      return val.compareTo(ic.val);
   }
   
   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return val.toString();
   }
}
