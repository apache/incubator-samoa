# Executing Apache SAMOA with Apache Gearpump

In this tutorial README we describe how to execute Apache SAMOA on top of [Apache Gearpump(incubating)](http://gearpump.apache.org/).

## Build

Simply clone the repository and install SAMOA.

```
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Pgearpump package
```

The deployable jar for SAMOA will be in `target/SAMOA-gearpump-0.4.0-incubating-SNAPSHOT.jar`.

## Executing SAMOA with Gearpump step-by-step

1. Ensure that you already have Gearpump running. You can follow this [tutorial](http://gearpump.apache.org/releases/latest/deployment-local.html) to deploy Gearpump in local mode.
2. Set `GEARPUMP_HOME` to point to your Gearpump installation path.
3. In the SAMOA path, you can input command to execute SAMOA tasks. For example, `bin/samoa gearpump target/SAMOA-gearpump-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 1000000 -f 100000 -l (classifiers.trees.VerticalHoeffdingTree -p 4) -s (generators.RandomTreeGenerator -c 2 -o 10 -u 10)"`


