package dbmigrate

class TriggerBuilder {
    def String triggerName, tableName, tableId, auditLogTable, keyColumn2
    def parent
    private columns

    /**
     * parent opts: table, tableId, descCol, childFk, useTimestamp=[bool]
     */
    TriggerBuilder(triggerName, tableName, tableId, keyColumn2='(NULL)', Map parent=null, auditLogTable='AuditLog') {
        this.triggerName = triggerName
        this.tableName = tableName
		this.tableId = tableId
        this.auditLogTable = auditLogTable
        this.parent = parent
        this.keyColumn2 = keyColumn2
        columns = []
    }

    /**
     * opts: column, nullable, table, tableId, status
     */
    def log(Map opts=[:], col) {
        opts.column = col
    	columns << opts
    }
    
    def log(col, table, tableId) {
    	columns << [column: col, nullable: false, table: table, tableId: tableId]
    }

    public String dropSql() {
        return "DROP TRIGGER IF EXISTS `${triggerName}`"
    } 

    private String caseDecoder(tableName, colName, decode) {
        def str = new StringBuilder()
        str << "(CASE "
        decode.each { key, value ->
            str << "WHEN ${tableName}.${colName} = '${key}' THEN '${value}' "
        }
        str << "WHEN ${tableName}.${colName} IS NULL THEN NULL END)"
        return str.toString()
    }

    public String createSql() {
        def str = new StringBuilder()
        def keyColumn3 = '(NULL)'
        def updatedBy = 'NEW.UpdatedBy'

        str << """
CREATE 
    TRIGGER `${triggerName}` BEFORE UPDATE ON `${tableName}` 
    FOR EACH ROW 
BEGIN
"""

        if (parent) {
            if (!parent.childFk) parent.childFk=parent.tableId
            str << """
    SET @desc = (SELECT ${parent.descCol} FROM ${parent.table} WHERE ${parent.tableId}=OLD.${parent.childFk});
"""
            keyColumn3 = '@desc'
        }

        if (parent && parent.useTimestamp) {
            str << """
    SET @updatedby = (SELECT UpdatedBy FROM ${parent.table} WHERE ${parent.tableId}=OLD.${parent.childFk});
"""
        } else {
            str << """
    SET @updatedby = NEW.UpdatedBy;
"""
        }

        columns.each { col -> 
            if (col.nullable) {
                str << """
    IF (NEW.${col.column} is NULL and OLD.${col.column} is not NULL) OR
       (NEW.${col.column} is not NULL and OLD.${col.column} is NULL) OR
       (OLD.${col.column} <> NEW.${col.column}) THEN
    """
            } else {
                str << """
    IF (OLD.${col.column} <> NEW.${col.column}) THEN
    """
            }

            if (col.status) {
                col.decode = ['1': 'Active', '2': 'Inactive']
            }

            if (col.table) {
                str << """
        INSERT INTO ${auditLogTable}
        ( 		TableName,
                PostDate,
                TransactionType,
                UserName,
                KeyColumn1,
                KeyColumn2,
                KeyColumn3,
                ColumnName,
                OldValue, OldDescription,
                NewValue, NewDescription)
        VALUES ( '${tableName}',
                NOW(),
                'U',
                @updatedby,
                OLD.${tableId},
                ${keyColumn2},
                ${keyColumn3},
                '${col.column}',
                OLD.${col.column},
                (Select ${col.label} FROM ${col.table} WHERE ${col.tableId}=OLD.${col.column}),
                NEW.${col.column},
                (Select ${col.label} FROM ${col.table} WHERE ${col.tableId}=NEW.${col.column}));
    """
            } else if (col.decode) {
                str << """
        INSERT INTO ${auditLogTable}
        ( 		TableName,
                PostDate,
                TransactionType,
                UserName,
                KeyColumn1,
                KeyColumn2,
                KeyColumn3,
                ColumnName,
                OldValue, OldDescription,
                NewValue, NewDescription)
        VALUES ( '${tableName}',
                NOW(),
                'U',
                @updatedby,
                OLD.${tableId},
                ${keyColumn2},
                ${keyColumn3},
                '${col.column}',
                OLD.${col.column},
                ${caseDecoder('OLD', col.column, col.decode)},
                NEW.${col.column},
                ${caseDecoder('NEW', col.column, col.decode)});
    """
            } else {
                str << """
        INSERT INTO ${auditLogTable}
        ( 		TableName,
                PostDate,
                TransactionType,
                UserName,
                KeyColumn1,
                KeyColumn2,
                KeyColumn3,
                ColumnName,
                OldValue,
                NewValue)
        VALUES ( '${tableName}',
                NOW(),
                'U',
                @updatedby,
                OLD.${tableId},
                ${keyColumn2},
                ${keyColumn3},
                '${col.column}',
                OLD.${col.column},
                NEW.${col.column});
    """
            }

            str << ' END IF;\n'

       }
					
		str << ' END; \n'
        return str.toString()
    }

    public static def createTrigger(String triggerName, String tableName, String tableId, 
                                    String keyColumn2='(NULL)', Map parent=null, closure) {
        def builder = new TriggerBuilder(triggerName, tableName, tableId, keyColumn2, parent)
        closure.delegate = builder
        closure()
        builder
    }

    public static void main(String[] args) {
    }
}
