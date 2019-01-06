package com.nebtrx.functional_actors

import cats.effect.Concurrent

trait MessageHandler[F[_], M[+ _], S] {
  def receive[A](state: S, msg: M[A], actor: Actor[F, M])(implicit sender: Actor[F, M],
                                                          c: Concurrent[F]): F[(S, A)]

}
