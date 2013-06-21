A small framework for hadoop streaming by Python

runner.py simplifies mapreduce code as well as its unit test written by Python.

Please refer to the wordcount example.

pydoop.py is a wrapper for hadoop cli, which can be used to do file operations and run mapreduce jobs easily, for example:

    # list files
    ./pydoop.py ls .

    # copy file from local to hadoop cluster
    ./pydoop.py put readme.md .

    # test wordcount job locally
    ./pydoop.py start -t readme.md output wordcount_mapper.py wordcount_reducer.py

    # run wordcount job in hadoop cluster
    ./pydoop.py start readme.md output wordcount_mapper.py wordcount_reducer.py runner.py

    # remove file from cluster
    ./pydoop.py rm readme.md

Please refer to this [blog post](http://blog.zhengdong.me/2012/07/30/streaming-python-unit-testing) for detail.
