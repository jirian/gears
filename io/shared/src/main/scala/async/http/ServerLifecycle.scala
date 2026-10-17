package gears.async.http

import gears.async.net.{TcpListener, TcpStream}
import gears.async.{Async, Cancellable, Future}

import scala.collection.mutable
import scala.util.Success

private[http] final class ServerLifecycle(limit: Int):
  private var listener: TcpListener | Null = null
  private var stopping = false
  private var forced = false
  private val connections = mutable.Map.empty[TcpStream, Boolean]
  private val handlers = mutable.Map.empty[TcpStream, Cancellable]
  private val waiters = mutable.ArrayBuffer.empty[Future.Promise[Unit]]

  def start(value: TcpListener): Unit = synchronized:
    if listener != null || stopping then throw IllegalStateException("A Server instance can serve only once")
    listener = value

  def add(stream: TcpStream): Boolean = synchronized:
    if stopping || connections.size >= limit then false
    else
      connections.update(stream, true)
      true

  def working(stream: TcpStream, active: Boolean): Boolean = synchronized:
    if stopping && !connections.getOrElse(stream, false) then false
    else if stopping && !active then false
    else
      connections.update(stream, active)
      true

  def attach(stream: TcpStream, handler: Cancellable): Unit =
    val cancel = synchronized:
      if connections.contains(stream) then handlers.update(stream, handler)
      forced
    if cancel then handler.cancel()

  def finished(stream: TcpStream): Unit =
    val notify = synchronized:
      connections.remove(stream)
      handlers.remove(stream)
      if connections.isEmpty then
        val all = waiters.toList
        waiters.clear()
        all
      else Nil
    notify.foreach(_.complete(Success(())))

  def isForced: Boolean = synchronized(forced)

  def stopAccepting()(using Async): Unit =
    val (bound, idle) = synchronized:
      stopping = true
      (listener, connections.iterator.collect { case (stream, false) => stream }.toList)
    if bound != null then bound.nn.close()
    idle.foreach: stream =>
      try stream.shutdownInput()
      catch case _: java.io.IOException => ()

  def stopNow(): Unit =
    val (bound, close, cancel) = synchronized:
      stopping = true
      forced = true
      (listener, connections.keys.toList, handlers.values.toList)
    if bound != null then bound.nn.close()
    close.foreach(_.close())
    cancel.foreach(_.cancel())

  def awaitDrained()(using Async): Unit =
    val promise = Future.Promise[Unit]()
    synchronized:
      if connections.isEmpty then promise.complete(Success(()))
      else waiters += promise
    promise.await
