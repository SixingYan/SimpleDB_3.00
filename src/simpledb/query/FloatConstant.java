package simpledb.query;

/**
 * The class that wraps Java ints as database constants.
 * @author Sixing Yan
 */
public class FloatConstant implements Constant {
   private float val;
   
   /**
    * Create a constant by wrapping the specified int.
    * @param n the int value
    */
   public FloatConstant(float n) {
      val = n;
   }
   
   /**
    * Unwraps the Integer and returns it.
    * @see simpledb.query.Constant#asJavaVal()
    */
   public Object asJavaVal() {
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
