# Objective
Convert workflows specified using https://wfcommons.org/format or  
https://github.com/wfcommons/wfformat to a workflow actionnable through dask

# Contents of the 'samples' directory 

## Contents of the 'samples/unittests' directory

Files necessary for the proper execution of unit tests

* hello-world-sequence.json:
The first task (hello) creates a file whose contents are used in the second task (world). 
The DAG is the following:
```commandline
(init) -> hello -> world -> (end)
```

* hello-world-join.json:
Three tasks in this workflow. The 'hello' and 'world' tasks each create a different file. Those files
are then used in the third 'join' task
The DAG is the following:
```commandline
      /-> hello -\
(init)            -> join -> (end)
      \-> world -/
```

# Contents of the 'samples/others' directory
* wrench directory
Samples from the Wrench project. See https://github.com/wrench-project/wrench

* makeflow-instances
Samples from https://github.com/wfcommons/makeflow-instances

* pegasus-instances
Samples from https://github.com/wfcommons/pegasus-instances

* nextflow-instances
Samples from https://github.com/wfcommons/nextflow-instances

# Unit tests/Coverage
## Unit tests
```commandline
python -m unittest -s tests
````

## Coverage
```commandline
pip install coverage
coverage run --source=. -m unittest discover -s tests/ && coverage html
```
