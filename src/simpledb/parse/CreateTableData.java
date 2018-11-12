package simpledb.parse;

import simpledb.record.Schema;

/**
 * Data for the SQL <i>create table</i> statement.
 * @author Edward Sciore
 */
public class CreateTableData {
    private String tblname;
    private Schema sch;
    private String keyfield = null;
    /**
     * Saves the table name and schema.
     */
    public CreateTableData(String tblname, Schema sch) {
        this.tblname = tblname;
        this.sch = sch;
    }
    /**
     * Saves the table name and schema.
     */
    public CreateTableData(String tblname, Schema sch, String fldname) {
        this.tblname = tblname;
        this.sch = sch;
        this.keyfield = fldname;
    }

    /**
     * Returns the name of the new table.
     * @return the name of the new table
     */
    public String tableName() {
        return tblname;
    }

    /**
     * Returns the schema of the new table.
     * @return the schema of the new table
     */
    public Schema newSchema() {
        return sch;
    }
    /**
     * Returns the schema of the new table.
     * @return the schema of the new table
     */
    public CreateIndexData hasPrimaryKey() {
        if (this.keyfield != null) {
            String idxname = tblname + "@" + HASH;
            return new CreateIndexData(idxname, tblname, keyfield);
        } else
            return null;
    }
}

