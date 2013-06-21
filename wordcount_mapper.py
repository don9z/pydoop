#!/usr/bin/env python
import runner

def mapper(line):
    words = line.split()
    for word in words:
        yield '%s\t%s' % (word, 1)

if __name__ == '__main__':
    runner.run_mapper(mapper)
