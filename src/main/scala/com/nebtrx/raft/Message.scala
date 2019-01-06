package com.nebtrx.raft

sealed trait Message[+ _]

final case class AppendEntry(term: Term, entry: Entry) extends Message[Unit]

final case class ProcessAppendResponse(currentTerm: Term, success: Boolean) extends Message[Unit]

final case class Vote(term: Term, candidateId: MemberId) extends Message[Unit]

final case class ProcessVoteResponse(term: Term, voteGranted: Boolean) extends Message[Unit]

final case class StartElection(startedOn: Long) extends Message[Unit]

final case class UpdateClusterSettings[F[_]](config: RaftClusterSettings[F]) extends Message[Unit]

case object SendHeartbeat extends Message[Unit]
