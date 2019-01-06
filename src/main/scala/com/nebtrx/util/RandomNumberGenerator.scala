package com.nebtrx.util

import cats.Applicative

trait RandomNumberGenerator[F[_]] {
  def nextInt(low: Int, high: Int): F[Int]
}

object RandomNumberGenerator {
  import java.util.Random

  def apply[F[_]](implicit A: Applicative[F]): RandomNumberGenerator[F] = (low: Int, high: Int) => {
    val r = new Random
    A.pure(r.nextInt(high - low) + low)
  }
}
