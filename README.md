# InDB-KDB

## Description

In-database implementations of the KDB machine learning algorithm

This is code I wrote for my Master's thesis, which compared the performance of an in-database implementation with a more convententional implementation where data is first extracted from the database to csv format and then processed. Implementations are low-pass and out-of-core, so are suitable for learning from big data.

Contains several implementations using various SQL techniques to compute the statistics and learn the Bayesian network structure.

## Dependencies

- Oracle database - code was built and tested on Oracle 11.2 using these JVMs
  - Oracle in-database JVM (JDK 1.6)
  - External JVM (JDK 1.8)
- Java libs
  - ojdbc7.jar
  - jopt-simple-5.0.3.jar

## References and sources

1. The KDB algorithm was originally proposed in this paper:<br><br>Sahami, M. (1996). Learning Limited Dependence Bayesian Classifiers. In KDD-96: Proceedings of the Second International Conference on Knowledge Discovery and Data Mining (pp. 335-338). Portland, Oregon.

2. This code is based on an (out-of-database) Java implementation designed and written by my supervisor and his colleagues, which is available from https://github.com/nayyarzaidi/fupla
