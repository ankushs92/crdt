## About 
This is a DeltaCRDT library for Scala. I couldn't find a ready to use library to work with CRDT's for my master thesis so I decided to write one from scratch
It is built using Akka

## Goal
At the moment I am still working on it and it is not ready to be used. The eventual goal is to have a stable implementation of GCounters, data aggregations like Sum, Avg, ReplicatedGrowableArray and GrowOnlySet.
All of these implementations will be based on Delta CRDT's. 

References  
[Efficient Synchronization of State-based CRDTs](https://arxiv.org/pdf/1803.02750.pdf)