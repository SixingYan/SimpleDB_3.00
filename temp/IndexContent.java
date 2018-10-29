/**
 * The content of every avaible index in this project.
 * This information is used by the query planner in order to
 * estimate the costs of using the index,
 * and to obtain the schema of the index records.
 * Its methods are essentially the same as those of Plan.
 * @author Sixing Yan
 */
public class IndexContent {
	private Schema sch;
	private tx;

	public IndexContent(sch,tx) {
		this.sch = sch;
		this.tx = tx;
	}
	public Index initIndex(String idxtype) {
		Index idx;
		if (idxtype.equals(LINEAR_HASH)) {

		}
		else
			idx = new HashIndex();
		return idx;
	} 
}