## About 
This is a DeltaCRDT library for Scala. I couldn't find a ready to use library to work with CRDT's(conflict free replicated data type) for my master thesis and so I decided to write one from scratch
It is built using Akka, and the replicas communicate via TCP.

## Goal
At the moment I am still working on it and it is not ready to be used. The eventual goal is to have a stable implementation of GCounters, data aggregations like Sum, Avg, ReplicatedGrowableArray and GrowOnlySet.
All of these implementations will be based on Delta CRDT's[1]

## References  
[1] [Efficient Synchronization of State-based CRDTs](https://arxiv.org/pdf/1803.02750.pdf)

[2] [Delta CRDT](https://arxiv.org/pdf/1603.01529.pdf)

[3] [Portfolio of CRDT's](https://hal.inria.fr/inria-00609399v1/document)