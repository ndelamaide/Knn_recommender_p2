# Milestone Description

[Milestone-2.pdf](./Milestone-2.pdf)

Note: Section 'Updates' lists the updates since the original release of the Milestone.

Mu has prepared a report template for your convenience here: [Report Template](./Milestone-2-QA-template.tex).

# Dependencies

````
    sbt >= 1.4.7
    openjdk@8
````

Should be available by default on ````iccluster028.iccluster.epfl.ch````. Otherwise, refer to each project installation instructions. Prefer working locally on your own machine, you will have less interference in your measurements from other students.

If you work on ````iccluster028.iccluster.epfl.ch````, you need to modify the PATH by default by adding the following line in ````~/.bashrc````:
````
    export PATH=$PATH:/opt/sbt/sbt/bin
````

If you have multiple installations of openjdk, you need to specify the one to use as JAVA_HOME, e.g. on OSX with
openjdk@8 installed through Homebrew, you would do:
````
    export JAVA_HOME="/usr/local/Cellar/openjdk@8/1.8.0+282";
````

# Dataset

Download [data-m2.zip](https://gitlab.epfl.ch/sacs/cs-449-sds-public/project/dataset/-/raw/main/data-m2.zip).

Unzip:
````
> unzip data-m2.zip
````

It should unzip into ````data/```` by default. If not, manually move ````ml-100k```` and ````ml-1m```` into ````data/````.


# Repository Structure

````src/main/scala/shared/predictions.scala````:
All the functionalities of your code for all questions should be defined there.
This code should then be used in the following applications and tests.

## Applications

    1. ````src/main/scala/optimizing/Optimizing.scala````: Output answers to questions **BR.X**.
    2. ````src/main/scala/distributed/Exact.scala````: Output answers to questions **EK.X**.
    3. ````src/main/scala/distributed/Approximate.scala````: Output answers to questions **AK.X**.
    4. ````src/main/scala/economics/Economics.scala````: Output answers to questions **E.X**

Applications are separate from tests to make it easier to test with different
inputs and permit outputting your answers and timings in JSON format for easier
grading.

## Unit Tests

Corresponding unit tests for each application (except Economics.scala):

````
    src/test/scala/optimizing/OptimizingTests.scala
    src/test/scala/distributed/ExactTests.scala
    src/test/scala/distributed/ApproximateTests.scala
````

Your tests should demonstrate how to call your code to obtain the answers of
the applications, and should make exactly the same calls as for the
applications above. This structure intentionally encourages you to put as
little as possible functionality in the application. This also gives the TA a
clear and regular structure to check its correctness.

# Usage

## Execute unit tests

````
     sbt "testOnly test.AllTests"
````

You should fill all tests and ensure they all succeed prior to submission.

## Run applications 

### Optimizing

````
sbt "runMain scaling.Optimizing --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json optimizing-100k.json --master local[1] --users 943 --movies 1682"
````

### Parallel Exact KNN

````
sbt "runMain distributed.Exact --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json exact-100k-4.json --k 10 --master local[4] --users 943 --movies 1682"
````

### Approximate KNN

````
sbt "runMain distributed.Approximate --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json approximate-100k-4-k10-r2.json --k 10 --master local[4] --users 943 --movies 1682 --partitions 10 --replication 2"
````

### Economics

````
sbt "runMain economics.Economics --json economics.json"
````

## Time applications

For all the previous applications, you can set the number of measurements for timings by adding the following option ````--num_measurements X```` where X is an integer. The default value is ````0````.

## IC Cluster

Test your application locally as much as possible and only test on the iccluster
once everything works, to keep the cluster and the driver node maximally available
for other students.

### Assemble Application for Spark Submit

````sbt clean````: clean up temporary files and previous assembly packages.

````sbt assembly````: create a new jar
````target/scala-2.11/m2_yourid-assembly-1.0.jar```` that can be used with
````spark-submit````.

Prefer packaging your application locally and upload the tar archive of your application
before running on cluster.

### Upload jar on Cluster 

````
    scp target/scala-2.11/m2_yourid-assembly-1.0.jar <username>@iccluster028.iccluster.epfl.ch:~
````

### Run on Cluster

See [config.sh](./config.sh) for HDFS paths to pre-uploaded train and test datasets to replace TRAIN and TEST, like in the example commands below:
 
#### When using ML-100k
````
spark-submit --class distributed.Exact --master yarn --conf "spark.dynamicAllocation.enabled=false" --num-executors 1 m2_yourid-assembly-1.0.jar --json exact-100k-1.json --train $ML100Ku2base --test $ML100Ku2test
````
#### When using ML-1m
````
spark-submit --class distributed.Exact --master yarn --conf "spark.dynamicAllocation.enabled=false" --num-executors 1 m2_yourid-assembly-1.0.jar --json exact-1m-1.json --train $ML1Mrbtrain --test $ML1Mrbtest --separator :: --k 300 --users 6040 --movies 3952
````

In order to keep results obtained with different parameters in different .json files, simply modify the corresponding parameter ("--json") passed and the values. For instance, with ```--num-executors 4``` : ```--json exact-1m-4.json```.
Note that when changing from ML-100k to ML-1M, the parameter ```--separator ::``` should be added, and the number of users and movies should be modified.

## Grading scripts


We will use the following scripts to grade your submission:

    1. ````./test.sh````: Run all unit tests.
    2. ````./run.sh````: Run all applications without timing measurements.
    3. ````./time.sh````: Run all timing measurements. 

All scripts will produce execution logs in the ````logs````
directory, including answers produced in the JSON format. Logs directories are
in the format ````logs/<scriptname>-<datetime>-<machine>/```` and include at
least an execution log ````log.txt```` as well as possible JSON outputs from
applications. 

Ensure all scripts run correctly locally before submitting. 

## Submission

Steps:

    1. Create a zip archive with all your code within  ````src/````, as well as your report: ````zip sciper1-sciper2.zip -r src/ report.pdf````
    2. Submit ````sciper1-sciper2.zip```` the TA for grading on
       https://cs449-submissions.epfl.ch:8083/m2 using the passcode you have previously received by email.

# References

Essential sbt: https://www.scalawilliam.com/essential-sbt/

Explore Spark Interactively (supports autocompletion with tabs!): https://spark.apache.org/docs/latest/quick-start.html

Scallop Argument Parsing: https://github.com/scallop/scallop/wiki

Spark Resilient Distributed Dataset (RDD): https://spark.apache.org/docs/3.0.1/api/scala/org/apache/spark/rdd/RDD.html

# Credits

Erick Lavoie (Design, Implementation, Tests)

Athanasios Xygkis (Requirements, Tests)
