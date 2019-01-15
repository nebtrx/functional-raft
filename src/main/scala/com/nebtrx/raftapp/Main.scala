package com.nebtrx.raftapp

import java.util.concurrent.Executors

import cats.data.OptionT
import cats.effect.{IO, _}
import cats.instances.list._
import cats.syntax.all._
import cats.syntax.timer._
import com.nebtrx.functional_actors._
import com.nebtrx.raft._
import com.nebtrx.util.RandomNumberGenerator
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.log4cats.{Logger, SelfAwareStructuredLogger}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object Main extends IOApp {

  def handler(logger: Logger[IO]): MessageHandler[IO, Message, RaftState[IO, Message]] =
    new MessageHandler[IO, Message, RaftState[IO, Message]] {
      private lazy val ec: ExecutionContextExecutor =
        ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
      private lazy val timer: Timer[IO] = IO.timer(ec)

      def receive[A](state: RaftState[IO, Message], msg: Message[A], actor: Actor[IO, Message])(
        implicit sender: Actor[IO, Message],
        c: Concurrent[IO]
      ): IO[(RaftState[IO, Message], A)] =
        msg match {
          case AppendEntry(term, _) =>
            trackRequestReceived(state)
              .flatMap(s => handleAppendEntry(s, term, actor))
              .map(s => (s, ()))

          case Vote(term, candidate) =>
            trackRequestReceived(state)
              .flatMap(s => handleVote(s, term, candidate, actor))
              .map(s => (s, ()))

          case ProcessVoteResponse(_, grantedVote) =>
            handleProcessVoteResponse(state, grantedVote, actor)
              .map(s => (s, ()))

          case ProcessAppendResponse(_, _) => IO.pure((state, ()))

          case StartElection(startedOn) =>
            handleStartElection(state, startedOn, actor)
              .map(s => (s, ()))

          case c: UpdateClusterSettings[IO] =>
            handleUpdateClusterSettings(state, c.config, actor)
              .map(s => (s, ()))

          case SendHeartbeat =>
            handleSendHeartbeat(state, actor)
              .map(s => (s, ()))
        }

      // Handlers ***************************

      private def handleAppendEntry[A](state: RaftState[IO, Message],
                                       term: Term,
                                       actor: Actor[IO, Message])(
        implicit sender: Actor[IO, Message]
      ): IO[RaftState[IO, Message]] =
        for {
          _ <- logger.info(s"[${state.id}]: Heartbeat received. Current Term $term")
          updatedState <- if (term > state.term)
                           // TODO: sync log
                           state.updateTerm(term).resetHeartbeatTimerFiber(None)
                         else
                           IO.pure(state)

          finalState <- startOrResetElectionTimerFiber(updatedState, actor, immediately = true)

          _ <- sendMessageAsync(sender, ProcessAppendResponse(finalState.term, success = true))(
                actor
              )
        } yield finalState

      private def handleVote[A](
        state: RaftState[IO, Message],
        term: Term,
        candidate: MemberId,
        actor: Actor[IO, Message]
      )(implicit sender: Actor[IO, Message]): IO[RaftState[IO, Message]] =
        for {
          voteResult <- processVoteRequest(state, term, candidate)
          (voteGranted, newState) = voteResult
          _ <- sendMessageAsync(sender, ProcessVoteResponse(newState.term, voteGranted))(actor)
        } yield newState

      private def handleProcessVoteResponse(state: RaftState[IO, Message],
                                            granted: Boolean,
                                            actor: Actor[IO, Message])(
        implicit sender: Actor[IO, Message]
      ): IO[RaftState[IO, Message]] =
        for {
          resultTuple <- IO.pure({
                          if (!state.isLeader) {
                            val stateWithUpdatedVotes = state.registerReceivedVote(granted)
                            if (hasEnoughVotesToBecomeLeader(stateWithUpdatedVotes))
                              (true, stateWithUpdatedVotes.setAsLeader)
                            else
                              (false, stateWithUpdatedVotes)
                          } else
                            (false, state)
                        })
          (becameLeader, voteCastedState) = resultTuple
          _ <- logger.info(
                s"[${voteCastedState.id}]: Received vote $granted for term ${voteCastedState.term}."
              )
          _ <- if (becameLeader)
                logger.info(
                  s"[${voteCastedState.id}]: Became Leader in term ${voteCastedState.term}."
                )
              else IO.unit
          finalState <- startHeartbeatTimerFiber(voteCastedState, actor)
        } yield finalState

      private def handleStartElection(state: RaftState[IO, Message],
                                      startedOn: Long,
                                      actor: Actor[IO, Message])(
        implicit sender: Actor[IO, Message]
      ): IO[RaftState[IO, Message]] =
        if (isNotLeaderAndElectionStartedAfterLastRequest(state, startedOn)) {
          for {
            updatedState <- IO.pure(state.startNewElection)
            _ <- logger.info(
                  s"[${updatedState.id}]: Starting election for term ${updatedState.term}."
                )
            _ <- updatedState.otherClusterMembers
                  .parTraverse(sendMessageAsync(_, Vote(updatedState.term, updatedState.id))(actor))
            finalState <- startOrResetElectionTimerFiber(updatedState, actor)
          } yield finalState
        } else {
          IO.pure(state)
        }

      private def handleUpdateClusterSettings(
        state: RaftState[IO, Message],
        cs: RaftClusterSettings[IO],
        actor: Actor[IO, Message]
      )(implicit sender: Actor[IO, Message]): IO[RaftState[IO, Message]] =
        for {
          _ <- logger.info(s"[${state.id}]: Received Raft Cluster Settings  $cs")
          updatedState = state.updateMembersAndRaftSettings(cs.otherMembers, cs.raftSettings)
          finalState <- startOrResetElectionTimerFiber(updatedState, actor)
        } yield finalState

      private def handleSendHeartbeat(state: RaftState[IO, Message],
                                      actor: Actor[IO, Message]): IO[RaftState[IO, Message]] = {
        val entry = Entry(state.term, state.id)
        state.otherClusterMembers
          .parTraverse(a => sendMessageAsync(a, AppendEntry(state.term, entry))(actor))
          .map(_ => state)
      }

      // Utils ***************************

      private def isNotLeaderAndElectionStartedAfterLastRequest(state: RaftState[IO, Message],
                                                                startedOn: Long): Boolean =
        !state.isLeader && state.lastRequestTime.forall(_ < startedOn)

      private def sendMessageAsync(actor: Actor[IO, Message], message: Message[_])(
        implicit sender: Actor[IO, Message]
      ): IO[Unit] =
        (actor ! message).start *> IO.pure(())

      private def sendMessageAsync(actor: Actor[IO, Message], messageBuilder: Long => Message[_])(
        implicit sender: Actor[IO, Message]
      ): IO[Unit] =
        for {
          timestamp <- timer.clock.realTime(MILLISECONDS)
          _ <- (actor ! messageBuilder(timestamp)).start
        } yield ()

      private def trackRequestReceived(st: RaftState[IO, Message]): IO[RaftState[IO, Message]] =
        for {
          timestamp <- timer.clock.realTime(MILLISECONDS)
          updatedState = st.logRequestReceived(timestamp)
        } yield updatedState

      private def processVoteRequest(state: RaftState[IO, Message],
                                     term: Term,
                                     candidate: MemberId): IO[(Boolean, RaftState[IO, Message])] =
        for {
          tuple <- IO.pure({
                    if (term <= state.term) {
                      (false, state)
                    } else {
                      state.getOrRegisterGivenVote(candidate, term)
                    }
                  })
          (voteGranted, _) = tuple
          _ <- logger.info(
                s"[${state.id}]: Term $term vote request for $candidate was $voteGranted."
              )
        } yield tuple

      private def hasEnoughVotesToBecomeLeader(state: RaftState[IO, Message]): Boolean =
        state.votesReceived > (state.otherClusterMembers.size + 1) / 2

      private def startOrResetElectionTimerFiber(
        state: RaftState[IO, Message],
        actor: Actor[IO, Message],
        immediately: Boolean = false
      )(implicit sender: Actor[IO, Message]) = {
        val interval = state.raftSettings.electionTimeout
        for {
          fiber <- startElectionTimer(interval, actor, immediately)
          finalState <- state.resetElectionTimerFiber(Some(fiber))
        } yield finalState
      }

      private def startHeartbeatTimerFiber(
        state: RaftState[IO, Message],
        actor: Actor[IO, Message]
      )(implicit sender: Actor[IO, Message]) = {
        val interval = state.raftSettings.heartbeatTimeout
        for {
          fiber <- startHeartbeatTimer(interval, actor)
          finalState <- state.resetHeartbeatTimerFiber(Some(fiber))
        } yield finalState
      }

      private def startElectionTimer(interval: FiniteDuration,
                                     actor: Actor[IO, Message],
                                     immediately: Boolean)(
        implicit sender: Actor[IO, Message]
      ): IO[Fiber[IO, Unit]] =
        timer
          .repeatAtFixedRate(if (immediately) 0.milli else interval,
                             interval,
                             sendMessageAsync(actor, t => StartElection(t)))
          .start

      private def startHeartbeatTimer(interval: FiniteDuration, actor: Actor[IO, Message])(
        implicit sender: Actor[IO, Message]
      ): IO[Fiber[IO, Unit]] =
        timer
          .repeatAtFixedRate(
            interval,
            sendMessageAsync(actor, SendHeartbeat)
          )
          .start
    }

  def finalizer: StateFinalizer[IO, RaftState[IO, Message]] =
    new StateFinalizer[IO, RaftState[IO, Message]] {
      override def dispose(state: RaftState[IO, Message])(implicit c: Concurrent[IO]): IO[Unit] = {
        val cancelComputation = for {
          electionCancelable <- OptionT.fromOption[IO](state.mElectionTimerFiber.map(_.cancel))
          heartbeatCancelable <- OptionT.fromOption[IO](state.mHeartbeatTimerFiber.map(_.cancel))
          _ <- OptionT.liftF[IO, Unit](electionCancelable)
          _ <- OptionT.liftF[IO, Unit](heartbeatCancelable)
        } yield ()
        cancelComputation.value *> IO.unit
      }
    }

  private def updateClusterSettings(a: Actor[IO, Message],
                                    clusterMembers: List[Actor[IO, Message]],
                                    lowerBoundary: Int,
                                    higherBoundary: Int): IO[Unit] =
    for {
      electionInterval <- RandomNumberGenerator.apply[IO].nextInt(lowerBoundary, higherBoundary)
      raftSettings = RaftSettings(electionInterval.milli, lowerBoundary.milli)
      _ <- a ! UpdateClusterSettings(
            RaftClusterSettings(raftSettings, clusterMembers.filterNot(_ == a))
          )
    } yield ()

  override def run(args: List[String]): IO[ExitCode] = {
    val clusterSize = 20
    val lowerTimeoutBoundary = 150
    val higherTimeoutBoundary = 300

    implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.unsafeCreate[IO]

    for {
      clusterMembers <- (1 to clusterSize).toList
                         .traverse(
                           i =>
                             Actor[IO, Message, RaftState[IO, Message]](RaftState(i, List.empty),
                                                                        handler(unsafeLogger),
                                                                        finalizer)
                         )

      _ <- clusterMembers
            .parTraverse(
              a =>
                updateClusterSettings(a,
                                      clusterMembers,
                                      lowerTimeoutBoundary,
                                      higherTimeoutBoundary)
            )

      _ <- IO.sleep(10.minutes)
      _ <- clusterMembers.traverse_(_.stop)
    } yield ExitCode.Success
  }
}
