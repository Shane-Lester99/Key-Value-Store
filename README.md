# Redis-like Key-Value Database Clone

This is a simplified key-value database implementation similar to Redis. The focus was to create a simple protocol that can be implemented in complicated systems for disaster recovery to provide fault tolerance using a simple primary backup strategy. The inspiration to create this protocol was from [this](https://css.csail.mit.edu/6.824/2014/papers/bressoud-hypervisor.pdf) academic paper which implemented this idea in hypervisors to provide automatic disaster recovery in virtual machines. 

## Protocol and System Architecture

The protocol was based on a periodic handoff of the primary server's data to a backup server kept in sync by a ping server. The ping server was the central manager of the two servers and would have the task of assigning roles to idle hardware. Although the ping server was a single point of failure if the protocol worked well a protocol like Paxos could be implemented for fault tolerance of the manager.

## Current State Of Project

Although all the core logic was implemented and all test cases were passed, the protocol turned out to be overly complex and impractical. 

Unfortunately, as more edge cases were found, the protocol's implementation became more and more complicated to the point where it is no longer feasible. A good protocol should stay relatively simple even with edge cases but this one became more and more complex.

The last update to this project was in December 2019.
