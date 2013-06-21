#!/usr/bin/env python
import sys
import subprocess

hadoop_home = # Please set hadoop home path
hadoop_streaming_jar = # Please set hadoop streaming jar path

hadoop_bin = '%s/bin/hadoop' % hadoop_home
hadoop_fs = '%s fs' % hadoop_bin
hadoop_streaming = '%s jar %s' % (hadoop_bin, hadoop_streaming_jar)
lzo_compression = '-inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat'

def print_usage():
    print 'Usages:'
    print '\tpydoop cat <path>'
    print '\tpydoop ls <path>'
    print '\tpydoop exists <path>'
    print '\tpydoop rm <path>'
    print '\tpydoop mkdir <path>'
    print '\tpydoop put <src_path> <dest_path>'
    print '\tpydoop get <src_path> <dest_path>'
    print '\tpydoop mv <src_path> <dest_path>'
    print '\tpydoop cp <src_path> <dest_path>'
    print '\tpydoop start [options] <input_path> <output_path> <mapper> <reducer> [dependencies]'
    print '\t\toptions: -d lzo compression, -t try run in local'
    return 1

def pydoop():
    if len(sys.argv) < 3:
        ret = print_usage()
    elif sys.argv[1] == 'cat':
        ret = cat(sys.argv[2])
    elif sys.argv[1] == 'ls':
        ret = ls(sys.argv[2])
    elif sys.argv[1] == 'exists':
        if exists(sys.argv[2]) == 0:
            print '%s exists!' % sys.argv[2]
        else:
            print '%s doesn\'t exist!' % sys.argv[2]
    elif sys.argv[1] == 'rm':
        ret = rm(sys.argv[2])
    elif sys.argv[1] == 'mkdir':
        ret = mkdir(sys.argv[2])

    elif len(sys.argv) < 4:
        ret = print_usage()
    elif sys.argv[1] == 'put':
        ret = put(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'get':
        ret = get(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'mv':
        ret = mv(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'cp':
        ret = cp(sys.argv[2], sys.argv[3])

    elif len(sys.argv) < 6:
        ret = print_usage()
    elif sys.argv[1] == 'start':
        if (sys.argv[2] == '-d' or sys.argv[2] == '-t') and len(sys.argv) < 7:
            ret = print_usage()
        elif sys.argv[2] == '-d':
            reducer_count = sys.argv[6].split(',', 1)
            if len(reducer_count) == 2:
                reducer, count = reducer_count
            else:
                reducer, count = reducer_count[0], 1
            ret = start(sys.argv[3], sys.argv[4], sys.argv[5], reducer,
                        sys.argv[7:], compression=True, reducer_num=int(count))
        elif sys.argv[2] == '-t':
            ret = start(sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6],
                        sys.argv[7:], try_run=True)
        else:
            reducer_count = sys.argv[5].split(',', 1)
            if len(reducer_count) == 2:
                reducer, count = reducer_count
            else:
                reducer, count = reducer_count[0], 1
            ret = start(sys.argv[2], sys.argv[3], sys.argv[4], reducer,
                        sys.argv[6:], reducer_num=int(count))

    else:
        print >> sys.stderr, 'ERROR: unknown pydoop command:', sys.argv[1]
        return print_usage()
    return ret

def run(command, try_run=False):
    print '---------Execute---------'
    print '%s' % command
    if not try_run:
        print '---------Output---------'
        # Flush to guarantee below print outputs first
        sys.stdout.flush()
        return subprocess.call(command, shell=True)
    else:
        return 0

def start(input_path, output_path, mapper, reducer, dependencies,
          compression=False, try_run=False, reducer_num=1):
    if try_run:
        if mapper.endswith('.py'):
            mapper = './%s' % mapper
        if reducer.endswith('.py'):
            reducer = './%s' % reducer
        command = 'find %s -type f -print0 | xargs -0 cat | %s | sort -k1,1 | %s > %s' % (input_path, mapper, reducer, output_path)
        return run(command)

    command = hadoop_streaming

    if reducer_num != 1:
        command += ' -D mapred.reduce.tasks=%d' % reducer_num

    if compression:
        command += ' %s' % lzo_compression

    depends = dependencies[:]
    command += ' -input %s -output %s' % (input_path, output_path)
    if mapper.endswith('.py'):
        command += ' -mapper %s' % mapper
        depends.append(mapper)
    else:
        command += ' -mapper "%s"' % mapper

    if reducer.endswith('.py'):
        command += ' -reducer %s' % reducer
        depends.append(reducer)
    else:
        command += ' -reducer "%s"' % reducer

    for depend in depends:
        command += ' -file %s' % depend

    ret = run(command)
    # If job failed but not by already exist output, rm output
    if ret != 0 and ret != 4:
        ret = rm(output_path)
    return ret

def cat(path):
    command = '%s -cat %s' % (hadoop_fs, path)
    return run(command)

def ls(path):
    command = '%s -ls %s' % (hadoop_fs, path)
    return run(command)

def exists(path):
    command = '%s -test -e %s' % (hadoop_fs, path)
    return run(command)

def rm(path):
    command = '%s -rmr %s' % (hadoop_fs, path)
    return run(command)

def mkdir(path):
    command = '%s -mkdir %s' % (hadoop_fs, path)
    return run(command)

def put(path1, path2):
    command = '%s -put %s %s' % (hadoop_fs, path1, path2)
    return run(command)

def get(path1, path2):
    command = '%s -get %s %s' % (hadoop_fs, path1, path2)
    return run(command)

def mv(path1, path2):
    command = '%s -mv %s %s' % (hadoop_fs, path1, path2)
    return run(command)

def cp(path1, path2):
    command = '%s -cp %s %s' % (hadoop_fs, path1, path2)
    return run(command)

if __name__ == '__main__':
    sys.exit(pydoop())
