import dbmigrate.*
import groovy.sql.Sql
import java.util.logging.*
import java.io.File

Logger.getLogger('groovy.sql.Sql').level = Level.FINE
migrateHome = ''
projectDir = ''

if (!System.getenv('DBMIGRATE_HOME')) {
    println 'Error: DBMIGRATE_HOME is not defined.'
    return
} else {
    migrateHome = System.getenv('DBMIGRATE_HOME')
}

if (!System.getenv('PROJECT_DIR')) {
    println 'Error: PROJECT_DIR is not defined.'
    return
} else {
    projectDir = System.getenv('PROJECT_DIR')
}

def cli = new CliBuilder(usage: 'migrate [-pmh] init|generate|run|data|triggers')

cli.h(longOpt: 'help', 'usage information')
cli.p(longOpt: 'properties', argName: 'propfile', args: 1, required: false,
      'configuration properties file (default: ./migrate.properties)')
cli.m(longOpt: 'migrations-dir', argName: 'directory', args: 1, required: false,
      'directory of migration scripts (default: ./migrations/)')
cli.l(longOpt: 'library-dir', argName: 'directory', args: 1, required: false,
      'directory containing jars to be included in classpath (default: ./lib/)')
cli.v(longOpt: 'version', argName: 'number', args: 1, required: false, 
      type: Integer.class, 'version number to migrate to')
cli.s(longOpt: 'sql', 'generate sql migration')
cli.d(longOpt: 'data-dir', argName: 'directory', args:1, required: false, 'data directory (default: ./data/)')
cli.t(longOpt: 'trigger-dir', argName: 'directory', args:1, required: false, 'trigger data directory (default: ./triggers/)')

def opt = cli.parse(args)
def args = opt.arguments()
if (args.size() < 1 || opt.h) {
    cli.usage()
    return
}

this.config = [
    properties : ((opt.p) ? opt.p : "${projectDir}/migrate.properties"),
    migrationsDir : ((opt.m) ? opt.m : "${projectDir}/migrations"),
    libDir: ((opt.l) ? opt.l : "${projectDir}/lib"),
    dataDir: ((opt.d) ? opt.d : "${projectDir}/data"),
    triggerDir: ((opt.t) ? opt.t : "${projectDir}/triggers"),
    generateSql: opt.s,
    version: opt.v
]

String.metaClass.tableize = {
    def words = []
    eachMatch(/[A-Z][a-z\d]*/) { words << it.toLowerCase() }
    (words) ? words.join('_') : delegate
}


String.metaClass.camelize = {
    split('_').collect({ 
        if (it.length() > 1) {
            return it[0].toUpperCase() + it[1..-1] 
        } else {
            return it.toUpperCase()
        }
    }).join()
}


def command = args.head()
args = args.tail()

switch (command) {
    case 'init' : initializeProject(args); break;
    case 'generate' : generateMigration(args); break;
    case 'run': runMigration(args); break;
    case 'reset': 
        this.config.version = '0'
        runMigration(args)
        this.config.version = null
        runMigration(args)
        break;
    case 'data': 
        try {
            new DataMigration().run(args, this); 
        } finally { }
        break
    case 'triggers':
        try {
            this.config.dataDir = this.config.triggerDir
            new DataMigration().run(args, this);
        } finally { }

        break;
    default: cli.usage(); println(command); return
}

private def getDbConfig() {
    static _dbConfig = null
    if (_dbConfig == null) {
        _dbConfig = new ConfigSlurper().parse(new File(config.properties).toURL())
    }

    return _dbConfig
}

def getConnection() { 
    static _connection = null
    if (_connection == null) {
        def dbconfig = this.dbConfig
        def lib = new File(config.libDir)
        def cl = this.class.classLoader.rootLoader

        if (lib.exists() && lib.directory) {
            lib.eachFileMatch(~/.*jar$/) { cl.addURL(it.toURL()) }
        }
        _connection = Sql.newInstance(dbconfig.url, dbconfig.username, 
                                      dbconfig.password, dbconfig.driver)
    }

    return _connection

}


private def getMigrationFiles() {
    def files = []
    new File(config.migrationsDir).eachFileMatch(MigrationFile.pattern) {
        files << new MigrationFile(it)
    }

    return files.sort()
}

private def initializeProject(args) {
    def destDir = (args.size() < 1) ? '.' : args[0]
    def ant = new AntBuilder()
    ant.copy(toDir: destDir) {
        dirset(dir:"${migrateHome}/conf/skel")
        fileset(dir:"${migrateHome}/conf/skel")
    }
    println "Initialized db migrate project in ${destDir}."
}

private def generateMigration(args) {
    if (args.size() < 1) {
        println 'Usage: migrate [-s] generate <MigrationName>'
        return;
    }
    def files = getMigrationFiles()
    def max = (files.empty) ? 0 : files[-1].number
    def name = args[0]
    def filename

    if (config.generateSql) {
        filename = String.format('%03d_%s.up.sql', max+1, name.tableize())
        new File(config.migrationsDir, filename).withWriter { it << '' }
        println "Generated ${filename}"

        filename = String.format('%03d_%s.down.sql', max+1, name.tableize())
        new File(config.migrationsDir, filename).withWriter { it << '' }
        println "Generated ${filename}"
    } else {
        filename = String.format('%03d_%s.groovy', max+1, name.tableize())
        new File(config.migrationsDir, filename).withWriter { writer ->
            writer << """
import groovy.sql.Sql
class ${name.camelize()} {
    def up(sql) {
    }

    def down(sql) {
    }
}
return ${name.camelize()}
"""
        }
        println "Generated ${filename}"
    }
}

private def runMigration(args) {
    def db = this.connection

    // get version from database
    def version = 0
    try {
        version = db.firstRow('select version from schema_info').version
    } catch (e) {
        db.execute 'create table schema_info(version int not null)'
        db.execute 'insert into schema_info(version) values(0)'
    }

    // get migration files
    def migrations = getMigrationFiles()

    // get desired version
    def desiredVersion = (config.version) ? (config.version as int) : migrations[-1].number
    def newVersion = version
    //println "From version ${version} to ${desiredVersion}"

    // enclose in a transaction
    db.connection.autoCommit = false

    try {
        if (version < desiredVersion) {         // migrate up
            range = (version+1)..desiredVersion
            migrations.each { f ->
                if (f.number in range) {
                    f.execute(db, MigrationDirection.UP)
                    newVersion = f.number
                }
            }
        } else if (version > desiredVersion) {  // migrate down
            range = version..(desiredVersion+1)
            migrations.reverse().each { f ->
                if (f.number in range) {
                    f.execute(db, MigrationDirection.DOWN)
                    newVersion = f.number - 1
                }
            }
        } else {
            println "Already in version ${desiredVersion}" 
        }

        db.execute 'update schema_info set version=?', [ newVersion ]
        db.commit()
    } catch (e) {
        //println "Error: ${e.message}"
        e.printStackTrace()
        newVersion = version
        db.rollback()
    }

    db.connection.autoCommit = true
    println "New version: ${newVersion}"
}
