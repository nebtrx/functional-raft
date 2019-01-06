# functional raft

[![Build Status](https://travis-ci.org/nebtrx/functional-raft.svg?branch=master)](https://travis-ci.org/nebtrx/functional-raft)

This is an implementation of the [Raft Consensus Algorithm][raft-website].
It relies on a [simplified functional actor system][functional-actors]
(assembled by me with inspiration from a couple of places) built on top of 
[fs2 queues][fs2-queues] and [cats-effects][cats-effects].

The purpose behind this was to **learn more/exercise my knowledge** about 
**cats-effects** and **concurrency** while having fun. 

## Implementation Roadmap & Progress

Since this is a toy project and I started working in the funnier segment, let's
do some planning to ensure its completion in a, hopefully, close future.

**Raft Consensus Algorithm:**
 - [x] Leader Election.  
 - [ ] Log replication.
 - [ ] Dynamic cluster members changes.                
 
**Software Engineering:**
 - [x] Functional actors using concurrency primitives.
 - [x] Logging.
 - [x] Add a code formatter tool. _([Scalafmt][scalafmt] FTW.)_
 - [x] Add CI integration. _([Travis][travis-ci] FTW.)_
 - [ ] Testing. _(Damn it!: I was too excited about getting a MVP.)_
 - [ ] Benchmarking against an implementation using akka actors. _(Because why not?)_
 
**Knowledge sharing:** 
 - [ ] Write a blog post. _(This is the hardest part. I'm super-lazy for writing.)_


[raft-website]: https://raft.github.io/
[functional-actors]: https://github.com/nebtrx/functional-actors 
[fs2-queues]: https://fs2.io/concurrency-primitives.html
[cats-effects]: https://github.com/typelevel/cats-effect
[scalafmt]:https://scalameta.org/scalafmt/
[travis-ci]:https://travis-ci.org/
