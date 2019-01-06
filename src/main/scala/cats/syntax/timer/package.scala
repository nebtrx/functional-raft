package cats.syntax

import cats.effect.Timer
import cats.syntax.apply._
import cats.syntax.flatMap._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS, _}

import cats.Monad

package object timer {

  implicit final class TimerSintax[F[_]](val timer: Timer[F]) extends AnyVal {
    def repeatAtFixedRate(period: FiniteDuration,
                          task: F[Unit])(implicit monad: Monad[F]): F[Unit] =
      timer.clock.realTime(MILLISECONDS).flatMap { start =>
        task *> timer.clock.realTime(MILLISECONDS).flatMap { finish =>
          val nextDelay: Long = period.toMillis - (finish - start)
          timer.sleep(nextDelay.millis) *> repeatAtFixedRate(period, task)
        }
      }

    def repeatAtFixedRate(initialDelay: FiniteDuration, period: FiniteDuration, task: F[Unit])(
      implicit monad: Monad[F]
    ): F[Unit] =
      timer.sleep(initialDelay) *> repeatAtFixedRate(period, task)
  }
}
