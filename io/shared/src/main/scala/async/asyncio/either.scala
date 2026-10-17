// from https://github.com/natsukagami/gears-io/blob/direct/shared/src/main/scala/either.scala

package gears.util

import scala.util.boundary

object either:
  type Label[-E, -T] = boundary.Label[Either[E, T]]

  inline def error[E](e: E)(using Label[E, Nothing]) = boundary.break(Left(e))

  inline def ok[T](v: T) = Right(v)

  inline def apply[E, T](inline body: Label[E, T] ?=> T): Either[E, T] =
    boundary:
      Right(body)

  extension [E, T](e: Either[E, T])
    inline def ?(using Label[E, Nothing]): T = e match
      case Left(ex)     => boundary.break(Left(ex))
      case Right(value) => value
