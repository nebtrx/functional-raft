package com.nebtrx.raft

import scala.concurrent.duration._

case class RaftSettings(electionTimeout: FiniteDuration = 1000.milli,
                        heartbeatTimeout: FiniteDuration = 1000.milli)
