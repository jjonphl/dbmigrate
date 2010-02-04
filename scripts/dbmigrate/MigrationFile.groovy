package dbmigrate

import groovy.sql.Sql
import java.util.regex.Pattern

class MigrationFile implements Comparable {
    File file
    int number
    private FileType type
    static Pattern pattern = ~/^(\d+)_(.+)\.(up\.sql|groovy)$/

    def MigrationFile(File f) {
        def match = MigrationFile.pattern.matcher(f.name)
        file = f
        number = match[0][1] as int
        type = (match[0][3] == 'groovy') ? FileType.GROOVY : FileType.SQL
    }

    def execute(Sql sql, MigrationDirection dir) {
        if (type == FileType.GROOVY) {
            GroovyShell shell = new GroovyShell()
            def klass = shell.evaluate(file.text)
            def instance = klass.newInstance()
            switch (dir) {
                case MigrationDirection.UP: 
                    instance.up(sql);
                    println "Migrating up ${file.name}"
                    break
                case MigrationDirection.DOWN: 
                    instance.down(sql)
                    println "Migrating down ${file.name}"
            }
        } else if (type == FileType.SQL) {
            def _file
            def msg
            switch (dir) {
                case MigrationDirection.UP: 
                    _file = file; 
                    msg = "Migrating up ${_file.name}"
                    break;
                case MigrationDirection.DOWN: 
                    def downUri = file.toURI().toString().
                                       replace('.up.sql', '.down.sql')
                    _file = new File(new URI(downUri))
                    msg = "Migrating down ${_file.name}"
            }
            if (_file.isFile()) {
                new SqlReader(_file).each { sql.execute it }
                println msg
            } else {
                println "Skipping ${_file.name}."
            }
        } else {
            throw new RuntimeException("What the fuck is ${file.name} ?")
        }
    }

    int compareTo(o) {
        return this.number <=> o.number
    }

    String toString() {
        number + ': ' + file.name
    }
}

