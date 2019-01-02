package com.nebtrx.raft

sealed trait Message[+ _]

final case class EntryAppended(term: Term, entry: Entry) extends Message[Unit]

final case class HandleAppendResponse(currentTerm: Term, success: Boolean) extends Message[Unit]

final case class VoteRequested(term: Term, candidateId: MemberId) extends Message[Unit]

final case class HandleVoteResponse(term: Term, voteGranted: Boolean) extends Message[Unit]

final case class ElectionStarted(startedOn: Long) extends Message[Unit]

final case class ClusterSettingsUpdated[F[_]](config: RaftClusterSettings[F]) extends Message[Unit]

case object SendHeartbeat extends Message[Unit]


