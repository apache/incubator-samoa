---
title: Building Apache SAMOA
layout: documentation
documentation: true
---
To build SAMOA to run on local mode, on your own computer without a cluster, is simple as cloning the repository and installing it.

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn package
```
The deployable jar for SAMOA will be in `target/SAMOA-Local-0.3.0-SNAPSHOT.jar`.

### Storm
Simply clone the repository and install SAMOA.

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Pstorm package
```

The deployable jar for SAMOA will be in `target/SAMOA-Storm-0.3.0-SNAPSHOT.jar`.

* [1.1 Executing SAMOA with Apache Storm](Executing-SAMOA-with-Apache-Storm.html)

### S4

If you want to compile SAMOA for Apache S4, you will need to install the S4 dependencies manually as explained in [Executing SAMOA with Apache S4](Executing-SAMOA-with-Apache-S4.html).

Once the dependencies are installed, you can simply clone the repository and install SAMOA.

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -P<variant> package # where variant is "storm" or "s4"

mvn -Pstorm,s4 package # e.g., to get both versions
```

The deployable jars for SAMOA will be in `target/SAMOA-<variant>-<version>-SNAPSHOT.jar`. For example, for S4 `target/SAMOA-S4-0.3.0-SNAPSHOT.jar`.

* [1.2 Executing SAMOA with Apache S4](Executing-SAMOA-with-Apache-S4.html)
