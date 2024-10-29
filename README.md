# What is Raft?
> Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems. We hope Raft will make consensus available to a wider audience, and that this wider audience will be able to develop a variety of higher quality consensus-based systems than are available today. 

[The Raft Consensus Algorithm](https://raft.github.io/)

# Hold on—what is consensus?
> Consensus is a fundamental problem in fault-tolerant distributed systems. Consensus involves multiple servers agreeing on values. Once they reach a decision on a value, that decision is final. Typical consensus algorithms make progress when any majority of their servers is available; for example, a cluster of 5 servers can continue to operate even if 2 servers fail. If more servers fail, they stop making progress (but will never return an incorrect result).

> Consensus typically arises in the context of replicated state machines, a general approach to building fault-tolerant systems. Each server has a state machine and a log. The state machine is the component that we want to make fault-tolerant, such as a hash table. It will appear to clients that they are interacting with a single, reliable state machine, even if a minority of the servers in the cluster fail. Each state machine takes as input commands from its log. In our hash table example, the log would include commands like set x to 3. A consensus algorithm is used to agree on the commands in the servers' logs. The consensus algorithm must ensure that if any state machine applies set x to 3 as the nth command, no other state machine will ever apply a different nth command. As a result, each state machine processes the same series of commands and thus produces the same series of results and arrives at the same series of states.

[The Raft Consensus Algorithm](https://raft.github.io/) 

# What is this repository?
This repository contains a very basic and primitive implementation of the raft algorithm wrapped up as a Go library. It is fault-tolerant and supports leader election with randomized election timeouts, log replication, hearbeats, and log persistence. It makes heavy use of Goroutines to responed to RPC calls and do various periodic background jobs. It doesn't support neither membership changes nor log compaction.

The library was coded as part of [Lab 2](http://nil.csail.mit.edu/6.824/2020/labs/lab-raft.html) in MIT's [6.824: Distributed Systems](http://nil.csail.mit.edu/6.824/2020/).

# Tests
To run the tests,
```sh
$ cd raft
$ go test
```
