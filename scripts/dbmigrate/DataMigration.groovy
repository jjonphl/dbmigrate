package dbmigrate
import java.util.regex.Pattern
import migrate

class DataMigration {

    def run(args, migrate m) {
        def cmd = args.head()
        args = args.tail()

        switch (cmd) {
            case 'load' : load(args, m); break
            case 'loadCsv' : loadCsv(args, m); break
            case 'clear': throw new RuntimeException('what the fuck') 
        }
    }

    def load(args, migrate m) {
        def files = []
        new File(m.config.dataDir).eachFileMatch(DataFile.pattern) {
            files << new DataFile(it)
        }
        files.sort().each { it.execute(m.connection) }
    }

    def loadCsv(args, migrate m) {
        def files = []
        new File('./data-migrate').eachFileMatch(DataFile.pattern) {
            files << new DataFile(it)
        }
        files.sort().each { it.executeMigration(m.connection) }
    }
    def clear(args, config) {

    }
}

private class DataFile implements Comparable {
    static Pattern pattern = ~/^(\d+)_(.+)\.(sql|csv|groovy)$/
    int number
    File file
    FileType type
    String name

    def DataFile(File f) {
        def match = DataFile.pattern.matcher(f.name)
        file = f
        number = match[0][1] as int
        name = match[0][2]
        switch (match[0][3]) {
            case 'sql' : type = FileType.SQL; break
            case 'groovy' : type = FileType.GROOVY; break
            case 'csv' : type = FileType.CSV
        }
    }

    def execute(sql) {
        sql.connection.autoCommit = false
        try {
            print "executing ${file.name}... "
            if (type == FileType.SQL) {
                new SqlReader(file).each { sql.execute it }
            } else if (type == FileType.CSV) {
                def csv = new CsvReader(file)
                def headers = csv.nextLine()
                if (headers) {
                    // table name pattern: 001_foo-bar-TableName.csv
                    def table = name.split(/-/)[-1]
                    def cols = headers.join(',')
                    def stmt = "insert into ${table}(${cols}) values"
                    csv.each { 
                        def values = it.collect { s ->
                            if (s.startsWith('#!')) {
                                return s.substring(2)
                            } else {
                                def str = s.replaceAll('\'', '\\\'')
                                return "'${str}'"
                            }
                        }.join(',')
                        sql.execute "${stmt}(${values})".toString()
                    }
                }
            } else if (type == FileType.GROOVY) {
                GroovyShell shell = new GroovyShell()
                def klass = shell.evaluate(file.text)
                klass.newInstance().load(sql)
            }
            println 'done'
            sql.commit()
        } catch (e) {
            sql.rollback()
            throw e
        }
    }

    def executeMigration(sql) {
        sql.connection.autoCommit = false
        try {
            println "executing ${file.name}... "
            if (type == FileType.SQL)
                new SqlReader(file).each { sql.execute it }
            else if (type == FileType.CSV) {
                def csv = new CsvReader(file)
                def headers = csv.nextLine()
                def ret = []
                csv.each {
                    def values = it.collect { s ->
                        ret << s
                    }
                }

                def auctionDetails = []
                def aucProcessor
                def count = 1
                ret.eachWithIndex() { obj, i ->
                    auctionDetails << obj
                    if((i+1)%14 == 0){
                        aucProcessor = new AuctionProcessor(auctionDetails, sql)
                        aucProcessor.process(count)
                        count++
                        auctionDetails = []
                    }
                }
            }
            println 'done'
            sql.commit()
        } catch (e) {
            sql.rollback()
            throw e
        }
    }
    int compareTo(other) {
        if (other instanceof DataFile) {
            return number <=> other.number
        }
        return -1
    }
}
