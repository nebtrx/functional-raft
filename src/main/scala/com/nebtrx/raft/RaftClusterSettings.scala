package com.nebtrx.raft

import com.nebtrx.functional_actors.Actor

case class RaftClusterSettings[F[_]](raftSettings: RaftSettings,
                                     otherMembers: List[Actor[F, Message]])
