package dbmigrate

import java.io.InputStream
import java.io.PushbackInputStream

class CsvReader {
    private InputStream s
    private int state = 0

    private final QUOTE = ~/"/
    private final NOT_QUOTE = ~/[^"]/
    private final COMMA = ~/,/
    private final NOT_COMMA = ~/[^,]/
    private final NEWLINE = ~/\n/
    private final NOT_NEWLINE = ~/[^\n]/

    def CsvReader(String filename) {
        this(new File(filename).newInputStream())
    }

    def CsvReader(File file) {
        this(file.newInputStream())
    }

    def CsvReader(InputStream stream) {
        s = new PushbackInputStream(stream)
    }

    def nextLine() {
        int input = 0
        char c
        def ret
        def value

        while (input != -1) {
            input = s.read()
            c = input as char

            if (state == 0) {
                if (ret) {
                    s.unread(input)
                    break
                }
                ret = []
                switch (c) {
                    case NEWLINE: return a
                    case COMMA: value = ''; state = 1; break
                    case QUOTE: value = ''; state = 3; break
                    default: assert !value; value = '' + c; state = 2
                }
            } else if (state == 1) {
                assert value != null
                ret << value
                switch (c) {
                    case COMMA: value = ''; break
                    case NEWLINE: state = 0; break
                    case QUOTE: value = ''; state = 3; break
                    default: value = '' + c; state = 2
                }
            } else if (state == 2) {
                switch (c) {
                    case COMMA: state = 1; break
                    case NEWLINE: ret << value; state = 0; break
                    case '\r': break
                    default: value <<= c
                }
            } else if (state == 3) {
                switch (c) {
                    case QUOTE: state = 4; break
                    default: value <<= c
                }
            } else if (state == 4) {
                switch (c) {
                    case QUOTE: value << '"'; state = 3; break
                    case COMMA: state = 1; break
                    default: state = 5
                }
            } else if (state == 5) { 
                // illegal state: xxx,"yyy"   ,...
                // trash characters until comma or newline
                switch (c) {
                    case COMMA: state = 1; break
                    case NEWLINE: ret << value; state = 0
                }
            }
        }

        return ret.collect { it as String }
    }


    def each(Closure c) {
        def line
        while ((line = nextLine())) c(line)
    }

    def close() {
        s.close()
    }

    public static void main(String[] args) {
        def divider = '-' * 75
        new CsvReader(args[0]).each { 
            def str = it.join(';')
            println "${divider}\n${str}\n${divider}" 
        }
    }
}
