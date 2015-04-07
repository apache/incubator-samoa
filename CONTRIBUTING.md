How to contribute?
=================

The best way to submit a contribution to Apache SAMOA is through a GitHub pull request.

Here is a guide to contribute to Apache SAMOA.

## Where is the source code?

SAMOA source code is maintained through **git**. The Apache SAMOA [git repository](git://git.apache.org/incubator-samoa.git) is the source of truth. Only SAMOA committers can push updates to this repo.

The Apache git repo is [mirrored to github](https://github.com/apache/incubator-samoa) for convenience. This mirror is read-only.

For writing contributions we suggest you start by [forking](https://help.github.com/articles/fork-a-repo) the [GitHub Apache SAMOA](https://github.com/apache/incubator-samoa) repository.

## How do I build the software?

We use [**maven**](http://maven.apache.org/): `mvn clean package`.

Note that this only builds the core artifacts of SAMOA (instances, API, local engine and test framework).

To build everything, including the integrations with various stream processing platforms use the "all" profile: `mvn clean package -Pall`.

You may also specify platform profiles individually: `-Pstorm`, `-Ps4`, `-Psamza`, `-Pthreads`.

## What rules should I follow in a code contribution?

### Coding convention

* All public classes and methods should have informative Javadoc comments.
* Do not use @author tags.
* Code should be formatted according to [Sun's conventions](http://www.oracle.com/technetwork/java/javase/documentation/codeconvtoc-136057.html), with the following exceptions:
	* **Use spaces for indentation, not tabs**.
	* **Indent two (2) spaces per level, not four (4)**.
	* **Line length limit is 120 chars, instead of 80 chars**.
* SAMOA includes an [eclipse-format.xml](https://github.com/apache/incubator-samoa/blob/master/eclipse-format.xml) formatting template that Eclipse or IntelliJ idea users might find convenient.
* Prefer qualified imports to wildcard imports.

### Tests

* Contributions should pass existing unit tests (`mvn test -Pall`).
* New unit tests should be provided to demonstrate bugs and fixes. JUnit 4 is our test framework.

### Organization of the code changes

The most natural way is to:

1. Create a new feature branch in your local git repo, e.g. using a JIRA ticket ID as the branch name.
2. Make code changes and separate the commits into logical units.
3. Submit a pull request from that feature branch.

## How do I submit a contribution?

Once you have a contribution that follows the coding convention, passes the tests, and is organized into logical commits, you may submit a pull request.

The recommended way is to submit a pull request **through GitHub**.

1. Get a JIRA reference:
	1. If your contribution has no JIRA ticket, create a new JIRA ticket, describing the issue (see [^1] for how to do this).
	2. Otherwise note the existing JIRA ticket ID ( e.g. `SAMOA-XX` )
1. Create a pull request from your contribution, and submit it through the github interface to the github SAMOA mirror repo. Make sure to **include the JIRA ticket ID in the description of the pull request**, so that the JIRA ticket is automatically updated. e.g.: `SAMOA-XX: Fixes XYZ`

The patch will be reviewed and voted upon, according to the [project's bylaws](http://samoa.incubator.apache.org/documentation/Bylaws.html).

[^1]: You need to be logged into ASF's JIRA and use the create button on [SAMOA's JIRA page](https://issues.apache.org/jira/browse/SAMOA/).
