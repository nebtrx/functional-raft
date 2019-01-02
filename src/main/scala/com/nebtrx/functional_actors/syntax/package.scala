package com.nebtrx.functional_actors

import cats.MonadError
import cats.syntax.flatMap._
import com.nebtrx.functional_actors.errors.ResultNotCollectedError

package object syntax {
  implicit final class MonadErrorThrowableSyntax[F[_], A](private val fa: F[A]) extends AnyVal {
    def collect[B](pf: PartialFunction[A, B])(implicit F: MonadError[F, Throwable]): F[B] =
      fa.flatMap {
        pf.andThen(F.pure)
          .applyOrElse(_, (a: A) => {
            F.raiseError[B](ResultNotCollectedError(a))
          })
      }
  }
}
