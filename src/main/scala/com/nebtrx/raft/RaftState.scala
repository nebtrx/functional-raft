package com.nebtrx.raft

import cats.{Applicative, Functor, Monad}
import cats.data.OptionT
import cats.syntax.functor._
import cats.effect.{CancelToken, Fiber}
import com.nebtrx.functional_actors.Actor

case class RaftState[F[_], M[+ _]](id: MemberId,
                                   otherClusterMembers: List[Actor[F, M]],
                                   term: Term = 0,
                                   voteRegistry: Map[Term, MemberId] = Map.empty,
                                   votesReceived: Int = 0,
                                   isLeader: Boolean = false,
                                   lastRequestTime: Option[Long] = None,
                                   raftSettings: RaftSettings = RaftSettings(),
                                   mElectionTimerFiber: Option[Fiber[F, Unit]] = None,
                                   mHeartbeatTimerFiber: Option[Fiber[F, Unit]] = None) {
  def startNewElection: RaftState[F, M] =
    this.copy(term = this.term + 1, votesReceived = 1, isLeader = false)

  def updateTerm(term: Term): RaftState[F, M] = if (this.term == term) this else this.copy(term = term)

  def resetElectionTimerFiber(mFiber: Option[Fiber[F, Unit]])
                             (implicit M: Monad[F]): F[RaftState[F, M]] = {
    resetFiber(mFiber, s => s.mElectionTimerFiber, (s, u) => s.copy(mElectionTimerFiber = u))
  }

  def resetHeartbeatTimerFiber(mFiber: Option[Fiber[F, Unit]])
                             (implicit M: Monad[F]): F[RaftState[F, M]] = {
    resetFiber(mFiber, s => s.mHeartbeatTimerFiber, (s, u) => s.copy(mHeartbeatTimerFiber = u))
  }

  def registerReceivedVote(granted: Boolean): RaftState[F, M] = {
    this.copy(votesReceived = this.votesReceived + (if (granted) 1 else 0))
  }

  def setAsLeader: RaftState[F, M] = this.copy(isLeader = true)

  def registerGivenVote(candidate: MemberId, term: Term): RaftState[F, M] =
    this.copy(voteRegistry = voteRegistry + (term -> candidate), term = term)

  def logRequestReceived(timestamp: Long): RaftState[F, M] = this.copy(lastRequestTime = Some(timestamp))

  def getOrRegisterGivenVote(candidate: MemberId, term: Term): (Boolean, RaftState[F, M]) = {
    this.voteRegistry.get(term).map(_ == candidate) match {
      case Some(vote) => (vote, this)
      case _          => (true, registerGivenVote(candidate, term))
    }
  }

  def updateMembersAndRaftSettings(members: List[Actor[F, M]], raftSettings: RaftSettings): RaftState[F, M] = {
    this.copy(otherClusterMembers = members, raftSettings = raftSettings)
  }

  override def toString: String = s"RaftState(id=$id, isLeader=$isLeader, currentTerm=$term, votes=$votesReceived, lastRequestTime=$lastRequestTime)"

  private def resetFiber(mFiber: Option[Fiber[F, Unit]],
                         getFiberFrom: RaftState[F, M] => Option[Fiber[F, Unit]],
                         updateFiberIn: (RaftState[F, M], Option[Fiber[F, Unit]]) => RaftState[F, M])
                        (implicit M: Monad[F]): F[RaftState[F, M]] = {
    (for {
      cancelable <- OptionT.fromOption[F](getFiberFrom(this).map(_.cancel))
      _ <- OptionT.liftF[F, Unit](cancelable)
    } yield this)
      .value
      .map(_.getOrElse(this))
      .map(updateFiberIn(_, mFiber))
  }

}
