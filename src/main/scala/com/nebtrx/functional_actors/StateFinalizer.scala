package com.nebtrx.functional_actors

import cats.effect.Concurrent

trait StateFinalizer[F[_], S] {
  def dispose(state: S)(implicit c: Concurrent[F]): F[Unit]
}
