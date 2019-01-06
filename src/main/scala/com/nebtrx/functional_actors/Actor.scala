package com.nebtrx.functional_actors

import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.syntax.concurrent._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.nebtrx.functional_actors.syntax._
import fs2.concurrent.Queue
import io.chrisdavenport.log4cats.Logger

trait Actor[F[_], M[+ _]] {

  def ![A](fa: M[A])(implicit sender: Actor[F, M] = this): F[A]

  def stop: F[Unit]

}

object Actor {

  private type ActorMessage[F[_], M[+ _], A] = (M[A], Deferred[F, A], Actor[F, M], Actor[F, M])

  def apply[F[_], M[+ _], S](initialState: S,
                             messageHandler: MessageHandler[F, M, S],
                             finalizer: StateFinalizer[F, S])(
    implicit c: Concurrent[F],
    logger: Logger[F]
  ): F[Actor[F, M]] = {

    def process[A](pendingMsg: ActorMessage[F, M, A],
                   stateRef: Ref[F, S])(implicit c: Concurrent[F]): F[Unit] =
      for {
        state <- stateRef.get
        (msg, deferred, actor, sender) = pendingMsg
        // Dummy trace to avoid a warning for unused logger since I wanna keep it just in case
        _ <- logger.trace(s"Processing received message")
        result <- messageHandler.receive(state, msg, actor)(sender, c)
        (newState, output) = result
        _ <- stateRef.set(newState) *> deferred.complete(output)
      } yield ()

    for {
      stateRef <- Ref.of[F, S](initialState)
      queue <- Queue.unbounded[F, ActorMessage[F, M, _]]
      consumer <- (for {
                   msg <- queue.dequeue1
                   _ <- process(msg, stateRef)
                 } yield ()).foreverM.void.start
    } yield
      new Actor[F, M] {
        def ![A](fa: M[A])(implicit sender: Actor[F, M]): F[A] =
          for {
            deferred <- Deferred[F, A]
            _ <- queue.offer1((fa, deferred, this, sender))
            output <- consumer.join
                       .race(deferred.get)
                       .collect { case Right(o) => o }
          } yield output

        override def stop: F[Unit] =
          for {
            state <- stateRef.get
            _ <- finalizer.dispose(state)
            _ <- consumer.cancel
          } yield ()
      }
  }
}
