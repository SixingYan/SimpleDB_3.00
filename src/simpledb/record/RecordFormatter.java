package simpledb.record;

import static java.sql.Types.INTEGER;
import static java.sql.Types.FLOAT;
import static simpledb.file.Page.*;
import static simpledb.record.RecordPage.EMPTY;
import simpledb.file.Page;
import simpledb.buffer.PageFormatter;

/**
 * An object that can format a page to look like a block of
 * empty records.
 * @author Edward Sciore | Sixing Yan
 */
class RecordFormatter implements PageFormatter {
    private TableInfo ti;

    /**
     * Creates a formatter for a new page of a table.
     * @param ti the table's metadata
     */
    public RecordFormatter(TableInfo ti) {
        this.ti = ti;
    }

    /**
     * Formats the page by allocating as many record slots
     * as possible, given the record length.
     * Each record slot is assigned a flag of EMPTY.
     * Each integer field is given a value of 0, and
     * each string field is given a value of "".
     * @see simpledb.buffer.PageFormatter#format(simpledb.file.Page)
     */
    public void format(Page page) {
        int recsize = ti.recordLength() + INT_SIZE;
        for (int pos = 0; pos + recsize <= BLOCK_SIZE; pos += recsize) {
            page.setInt(pos, EMPTY);
            makeDefaultRecord(page, pos);
        }
    }
    /**
     * Add the support for set a float value into page
     * @param page
     * @param pos
     */
    private void makeDefaultRecord(Page page, int pos) {
        for (String fldname : ti.schema().fields()) {
            int offset = ti.offset(fldname);
            if (ti.schema().type(fldname) == INTEGER)
                page.setInt(pos + INT_SIZE + offset, 0);
            else if (ti.schema().type(fldname) == FLOAT)
                page.setFloat(pos + INT_SIZE + offset, 0f);
            else
                page.setString(pos + INT_SIZE + offset, "");
        }
    }
}
