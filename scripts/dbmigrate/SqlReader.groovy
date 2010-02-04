package dbmigrate
import java.io.InputStream
import java.io.PushbackInputStream

class SqlReader {
    private InputStream s
    private int state = 0
    private def stmt = null

    private final WS = ~/\s/
    private final TERM = ~/;/
    private final NOT_TERM = ~/[^;]/
    private final STR = ~/'/
    private final NOT_STR = ~/[^']/

    def SqlReader(String filename) {
        this(new File(filename).newInputStream())
    }

    def SqlReader(File file) {
        this(file.newInputStream())
    }

    def SqlReader(InputStream stream) {
        s = new PushbackInputStream(stream)
    }

    String nextStatement() {
        int input = 0
        char c
        stmt = ''

        def found = false
        while (!found) {
            input = s.read()
            if (input == -1) break
            c = input as char
            if (state == 0) {
                switch (c) {
                    case WS: state = 1; break
                    case TERM: found = true; break
                    case '-' : state = 7; break
                    default: stmt <<= c; state = 2
                }
            } else if (state == 1) {
                switch (c) {
                    case WS: break
                    case TERM: found = true; break
                    case '-': state = 7; break
                    default: stmt <<= c; state = 2
                }
            } else if (state == 2) {
                switch (c) {
                    case TERM: 
                        stmt <<= c; found = true; break 
                    case NOT_TERM: 
                        stmt <<= c; break
                    case STR: 
                        stmt <<= c; state = 3; break
                    case '-':
                        state = 5; break
                    default:
                        stmt <<= c
                }
            } else if (state == 3) {   // inside a sql string
                switch (c) {
                    case STR: // closing string quote quote
                        stmt <<= c; state = 2; break
                    case '\\':  // escape sequence
                        stmt <<= c; state = 4; break
                    case NOT_STR:
                        stmt <<= c
                }
            } else if (state == 4) {   // escape sequence
                stmt <<= c
                state = 3
            } else if (state == 5) {
                switch (c) {
                    case '-': state = 6; break
                    default: stmt <<= '-' + c; state = 2
                }
            } else if (state == 6) {
                switch (c) {
                    case '\n': state = 2; break
                }
            } else if (state == 7) {
                switch (c) {
                    case '-': state = 8; break
                    default:
                        stmt <<= '-' + c; state = 2
                }
            } else if (state == 8) {
                switch (c) {
                    case '\n': state = 2
                }
            }
        }

        if (stmt) {
            state = 0
            return stmt
        } else {
            return null
        }
    }

    def each(Closure c) {
        def stmt
        while ((stmt = nextStatement())) c(stmt)
    }

    def close() {
        s.close()
    }

    public static void main(String[] args) {
        def divider = '-' * 75
        new SqlReader(args[0]).each { println "${divider}\n${it}\n${divider}" }
    }
}
