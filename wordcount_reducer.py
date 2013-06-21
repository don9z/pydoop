#!/usr/bin/env python
import runner
def reducer(key, values):
    sum = 0
    for value in values:
        try:
            count = int(value[0])
        except ValueError:
            continue
        sum += count
    yield '%s\t%s' % (key, sum)

if __name__ == '__main__':
    runner.run_reducer(reducer)
