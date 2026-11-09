// from https://github.com/armanbilge/fs2-io_uring/blob/main/uring/src/main/scala/fs2/io/uring/unsafe/uring.scala
// Modified for gears: moved to package gears.async.io.uring, linked with @link("uring"), shim symbols renamed to
// gears_*, and fs2's unused bindings removed (close, connect, shutdown and socket preparations, user data, timed waits).
// Ours, not fs2's: the poll_add, read, write, readv, writev, fsync, openat, sendmsg and recvmsg preparations; the
// synchronous cancels (cancel_fd, cancel_now); a submit and wait in one call; io_uring_sq_ready, get_events, size,
// features and cq_overflowing. The struct io_uring mirror only types the pointer: the reactor allocates io_uring_size()
// bytes for it, so a liburing whose struct grows cannot be written past.

/*
 * Copyright 2022 Arman Bilge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gears.async.io
package uring

import scala.scalanative.libc.stddef._
import scala.scalanative.posix.sys.socket._
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** liburing's functions and the kernel's io_uring structures. The three helpers liburing only has as `static inline`,
  * and a synchronous cancel, come from `uring_shim.c` under `gears_` names.
  */
@link("uring")
@extern
private[uring] object uring {
  final val IORING_OP_NOP = 0
  final val IORING_OP_READV = 1
  final val IORING_OP_WRITEV = 2
  final val IORING_OP_FSYNC = 3
  final val IORING_OP_POLL_ADD = 6
  final val IORING_OP_SENDMSG = 9
  final val IORING_OP_RECVMSG = 10
  final val IORING_OP_ACCEPT = 13
  final val IORING_OP_ASYNC_CANCEL = 14
  final val IORING_OP_OPENAT = 18
  final val IORING_OP_READ = 22
  final val IORING_OP_WRITE = 23
  final val IORING_OP_SEND = 26
  final val IORING_OP_RECV = 27

  /** A send's or a receive's flag (in `ioprio`): wait for readiness before trying, for one just tried without waiting.
    */
  final val IORING_RECVSEND_POLL_FIRST = 1

  type __u8 = CUnsignedChar
  type __u16 = CUnsignedShort
  type __s32 = CInt
  type __u32 = CUnsignedInt
  type __u64 = CUnsignedLongLong

  type __kernel_time64_t = CLongLong
  type __kernel_timespec = CStruct2[__kernel_time64_t, CLongLong]

  type __kernel_rwf_t = CUnsignedInt

  type iovec = CStruct2[Ptr[Byte], size_t]
  type msghdr = CStruct7[Ptr[Byte], CUnsignedInt, Ptr[iovec], size_t, Ptr[Byte], size_t, CInt]

  type io_uring = CStruct9[
    io_uring_sq,
    io_uring_cq,
    CUnsignedInt,
    CInt,
    CUnsignedInt,
    CInt,
    __u8,
    CArray[__u8, Nat._3],
    CUnsignedInt
  ]

  type io_uring_cq = CStruct12[
    Ptr[CUnsignedInt],
    Ptr[CUnsignedInt],
    Ptr[CUnsignedInt],
    Ptr[
      CUnsignedInt
    ],
    Ptr[CUnsignedInt],
    Ptr[CUnsignedInt],
    Ptr[io_uring_cqe],
    size_t,
    Ptr[
      Byte
    ],
    CUnsignedInt,
    CUnsignedInt,
    CArray[CUnsignedInt, Nat._2]
  ]

  type io_uring_cqe = CStruct3[__u64, __s32, __u32]

  type io_uring_sq = CStruct15[
    Ptr[CUnsignedInt],
    Ptr[CUnsignedInt],
    Ptr[CUnsignedInt],
    Ptr[
      CUnsignedInt
    ],
    Ptr[CUnsignedInt],
    Ptr[CUnsignedInt],
    Ptr[CUnsignedInt],
    Ptr[
      io_uring_sqe
    ],
    CUnsignedInt,
    CUnsignedInt,
    size_t,
    Ptr[Byte],
    CUnsignedInt,
    CUnsignedInt,
    CArray[
      CUnsignedInt,
      Nat._2
    ]
  ]

  type io_uring_sqe =
    CStruct10[__u8, __u8, __u16, __s32, __u64, __u64, __u32, __u32, __u64, CArray[__u64, Nat._3]]

  @name("gears_io_uring_init")
  def io_uring_init(entries: CUnsignedInt, ring: Ptr[io_uring], completions: CUnsignedInt): CInt = extern

  def io_uring_queue_exit(ring: Ptr[io_uring]): Unit = extern

  @name("gears_io_uring_get_sqe")
  def io_uring_get_sqe(ring: Ptr[io_uring]): Ptr[io_uring_sqe] = extern

  def io_uring_submit(ring: Ptr[io_uring]): CInt = extern

  @name("gears_io_uring_sq_ready")
  def io_uring_sq_ready(ring: Ptr[io_uring]): CUnsignedInt = extern

  @name("gears_io_uring_submit_and_wait")
  @blocking
  def io_uring_submit_and_wait(ring: Ptr[io_uring], nanoseconds: CLongLong): CInt = extern

  def io_uring_peek_batch_cqe(
      ring: Ptr[io_uring],
      cqes: Ptr[Ptr[io_uring_cqe]],
      count: CUnsignedInt
  ): CUnsignedInt = extern

  @name("gears_io_uring_cq_advance")
  def io_uring_cq_advance(ring: Ptr[io_uring], nr: CUnsignedInt): Unit = extern

  @name("gears_io_uring_cancel_now")
  @blocking
  def io_uring_cancel_now(ring: Ptr[io_uring], user_data: __u64, flags: CInt, wait: CInt): CInt = extern

  @name("gears_io_uring_cancel_fd")
  @blocking
  def io_uring_cancel_fd(ring: Ptr[io_uring], fd: CInt): CInt = extern

  final val IORING_ASYNC_CANCEL_ANY = 1 << 2

  @name("gears_io_uring_size")
  def io_uring_size(): CSize = extern

  @name("gears_io_uring_features")
  def io_uring_features(ring: Ptr[io_uring]): CUnsignedInt = extern

  @name("gears_io_uring_cq_overflowing")
  def io_uring_cq_overflowing(ring: Ptr[io_uring]): CInt = extern

  @name("gears_io_uring_get_events")
  def io_uring_get_events(ring: Ptr[io_uring]): CInt = extern

  final val IORING_FEAT_NODROP = 1 << 1
  final val IORING_FEAT_EXT_ARG = 1 << 8

}

/** liburing's `static inline` prep helpers, and field accessors for the `CStruct`s above. */
private[uring] object uringStructOps {

  import uring._

  def io_uring_prep_rw(
      op: Int,
      sqe: Ptr[io_uring_sqe],
      fd: Int,
      addr: Ptr[?],
      len: CUnsignedInt,
      offset: __u64
  ): Unit = {
    sqe.opcode = op.toUByte
    sqe.flags = 0.toUByte
    sqe.ioprio = 0.toUShort
    sqe.fd = fd
    sqe.off = offset
    sqe.addr = if (addr eq null) 0.toULong else addr.toLong.toULong
    sqe.len = len
    sqe.rw_flags = 0.toUInt
    sqe.__pad2(0) = 0.toULong
    sqe.__pad2(1) = 0.toULong
    sqe.__pad2(2) = 0.toULong
  }

  def io_uring_prep_nop(sqe: Ptr[io_uring_sqe]): Unit =
    io_uring_prep_rw(IORING_OP_NOP, sqe, -1, null, 0.toUInt, 0.toULong)

  def io_uring_prep_accept(
      sqe: Ptr[io_uring_sqe],
      fd: CInt,
      addr: Ptr[sockaddr],
      addrlen: Ptr[socklen_t],
      flags: CInt
  ): Unit = {
    io_uring_prep_rw(
      IORING_OP_ACCEPT,
      sqe,
      fd,
      addr,
      0.toUInt,
      if (addrlen eq null) 0.toULong else addrlen.toLong.toULong
    )
    sqe.accept_flags = flags.toUInt
  }

  def io_uring_prep_cancel64(sqe: Ptr[io_uring_sqe], user_data: __u64, flags: CInt): Unit = {
    io_uring_prep_rw(IORING_OP_ASYNC_CANCEL, sqe, -1, null, 0.toUInt, 0.toULong);
    sqe.addr = user_data;
    sqe.cancel_flags = flags.toUInt
  }

  def io_uring_prep_fsync(sqe: Ptr[io_uring_sqe], fd: CInt): Unit =
    io_uring_prep_rw(IORING_OP_FSYNC, sqe, fd, null, 0.toUInt, 0.toULong)

  def io_uring_prep_poll_add(sqe: Ptr[io_uring_sqe], fd: CInt, mask: CUnsignedInt): Unit = {
    io_uring_prep_rw(IORING_OP_POLL_ADD, sqe, fd, null, 0.toUInt, 0.toULong)
    sqe.poll32_events = mask
  }

  def io_uring_prep_read(
      sqe: Ptr[io_uring_sqe],
      fd: CInt,
      buf: Ptr[Byte],
      nbytes: CUnsignedInt,
      offset: __u64
  ): Unit = io_uring_prep_rw(IORING_OP_READ, sqe, fd, buf, nbytes, offset)

  def io_uring_prep_write(
      sqe: Ptr[io_uring_sqe],
      fd: CInt,
      buf: Ptr[Byte],
      nbytes: CUnsignedInt,
      offset: __u64
  ): Unit = io_uring_prep_rw(IORING_OP_WRITE, sqe, fd, buf, nbytes, offset)

  def io_uring_prep_readv(
      sqe: Ptr[io_uring_sqe],
      fd: CInt,
      iovecs: Ptr[iovec],
      nr_vecs: CUnsignedInt,
      offset: __u64
  ): Unit = io_uring_prep_rw(IORING_OP_READV, sqe, fd, iovecs, nr_vecs, offset)

  def io_uring_prep_writev(
      sqe: Ptr[io_uring_sqe],
      fd: CInt,
      iovecs: Ptr[iovec],
      nr_vecs: CUnsignedInt,
      offset: __u64
  ): Unit = io_uring_prep_rw(IORING_OP_WRITEV, sqe, fd, iovecs, nr_vecs, offset)

  def io_uring_prep_openat(
      sqe: Ptr[io_uring_sqe],
      dfd: CInt,
      path: CString,
      flags: CInt,
      mode: CUnsignedInt
  ): Unit = {
    io_uring_prep_rw(IORING_OP_OPENAT, sqe, dfd, path.asInstanceOf[Ptr[Byte]], mode, 0.toULong)
    sqe.open_flags = flags.toUInt
  }

  def io_uring_prep_send(sqe: Ptr[io_uring_sqe], fd: CInt, buf: Ptr[Byte], len: CUnsignedInt, flags: CInt): Unit = {
    io_uring_prep_rw(IORING_OP_SEND, sqe, fd, buf, len, 0.toULong)
    sqe.msg_flags = flags.toUInt
  }

  def io_uring_prep_recv(sqe: Ptr[io_uring_sqe], fd: CInt, buf: Ptr[Byte], len: CUnsignedInt, flags: CInt): Unit = {
    io_uring_prep_rw(IORING_OP_RECV, sqe, fd, buf, len, 0.toULong)
    sqe.msg_flags = flags.toUInt
  }

  def io_uring_prep_sendmsg(
      sqe: Ptr[io_uring_sqe],
      fd: CInt,
      msg: Ptr[msghdr],
      flags: CInt
  ): Unit = {
    io_uring_prep_rw(IORING_OP_SENDMSG, sqe, fd, msg.asInstanceOf[Ptr[Byte]], 1.toUInt, 0.toULong)
    sqe.msg_flags = flags.toUInt
  }

  def io_uring_prep_recvmsg(
      sqe: Ptr[io_uring_sqe],
      fd: CInt,
      msg: Ptr[msghdr],
      flags: CInt
  ): Unit = {
    io_uring_prep_rw(IORING_OP_RECVMSG, sqe, fd, msg.asInstanceOf[Ptr[Byte]], 1.toUInt, 0.toULong)
    sqe.msg_flags = flags.toUInt
  }

  implicit final class io_uring_sqeOps(val io_uring_sqe: Ptr[io_uring_sqe]) extends AnyVal {
    inline def opcode: __u8 = io_uring_sqe._1
    inline def opcode_=(opcode: __u8): Unit = !io_uring_sqe.at1 = opcode

    inline def flags: __u8 = io_uring_sqe._2
    inline def flags_=(flags: __u8): Unit = !io_uring_sqe.at2 = flags

    inline def ioprio: __u16 = io_uring_sqe._3
    inline def ioprio_=(ioprio: __u16): Unit = !io_uring_sqe.at3 = ioprio

    inline def fd: __s32 = io_uring_sqe._4
    inline def fd_=(fd: __s32): Unit = !io_uring_sqe.at4 = fd

    inline def off: __u64 = io_uring_sqe._5
    inline def off_=(off: __u64): Unit = !io_uring_sqe.at5 = off

    inline def addr: __u64 = io_uring_sqe._6
    inline def addr_=(addr: __u64): Unit = !io_uring_sqe.at6 = addr

    inline def len: __u32 = io_uring_sqe._7
    inline def len_=(len: __u32): Unit = !io_uring_sqe.at7 = len

    inline def rw_flags: __kernel_rwf_t = io_uring_sqe._8
    inline def rw_flags_=(rw_flags: __kernel_rwf_t): Unit = !io_uring_sqe.at8 = rw_flags
    inline def msg_flags: __u32 = io_uring_sqe._8
    inline def msg_flags_=(msg_flags: __u32): Unit = !io_uring_sqe.at8 = msg_flags
    inline def accept_flags: __u32 = io_uring_sqe._8
    inline def accept_flags_=(accept_flags: __u32): Unit = !io_uring_sqe.at8 = accept_flags
    inline def cancel_flags: __u32 = io_uring_sqe._8
    inline def cancel_flags_=(cancel_flags: __u32): Unit = !io_uring_sqe.at8 = cancel_flags
    inline def poll32_events: __u32 = io_uring_sqe._8
    inline def poll32_events_=(poll32_events: __u32): Unit = !io_uring_sqe.at8 = poll32_events
    inline def open_flags: __u32 = io_uring_sqe._8
    inline def open_flags_=(open_flags: __u32): Unit = !io_uring_sqe.at8 = open_flags

    inline def user_data: __u64 = io_uring_sqe._9
    inline def user_data_=(user_data: __u64): Unit = !io_uring_sqe.at9 = user_data

    inline def __pad2: CArray[__u64, Nat._3] = io_uring_sqe._10
  }

  implicit final class io_uring_cqeOps(val io_uring_cqe: Ptr[io_uring_cqe]) extends AnyVal {
    inline def user_data: __u64 = io_uring_cqe._1
    inline def user_data_=(user_data: __u64): Unit = !io_uring_cqe.at1 = user_data
    inline def res: __s32 = io_uring_cqe._2
    inline def res_=(res: __s32): Unit = !io_uring_cqe.at2 = res
    inline def flags: __u32 = io_uring_cqe._3
    inline def flags_=(flags: __u32): Unit = !io_uring_cqe.at3 = flags
  }

  implicit final class __kernel_timespecOps(val __kernel_timespec: Ptr[__kernel_timespec]) extends AnyVal {
    inline def tv_sec: __kernel_time64_t = __kernel_timespec._1
    inline def tv_sec_=(tv_sec: __kernel_time64_t): Unit = !__kernel_timespec.at1 = tv_sec
    inline def tv_nsec: CLongLong = __kernel_timespec._2
    inline def tv_nsec_=(tv_nsec: CLongLong): Unit = !__kernel_timespec.at2 = tv_nsec
  }

  implicit final class iovecOps(val iovec: Ptr[iovec]) extends AnyVal {
    inline def iov_base: Ptr[Byte] = iovec._1
    inline def iov_base_=(iov_base: Ptr[Byte]): Unit = !iovec.at1 = iov_base
    inline def iov_len: size_t = iovec._2
    inline def iov_len_=(iov_len: size_t): Unit = !iovec.at2 = iov_len
  }

  implicit final class msghdrOps(val msghdr: Ptr[msghdr]) extends AnyVal {
    inline def msg_name: Ptr[Byte] = msghdr._1
    inline def msg_name_=(msg_name: Ptr[Byte]): Unit = !msghdr.at1 = msg_name
    inline def msg_namelen: CUnsignedInt = msghdr._2
    inline def msg_namelen_=(msg_namelen: CUnsignedInt): Unit = !msghdr.at2 = msg_namelen
    inline def msg_iov: Ptr[iovec] = msghdr._3
    inline def msg_iov_=(msg_iov: Ptr[iovec]): Unit = !msghdr.at3 = msg_iov
    inline def msg_iovlen: size_t = msghdr._4
    inline def msg_iovlen_=(msg_iovlen: size_t): Unit = !msghdr.at4 = msg_iovlen
    inline def msg_control: Ptr[Byte] = msghdr._5
    inline def msg_control_=(msg_control: Ptr[Byte]): Unit = !msghdr.at5 = msg_control
    inline def msg_controllen: size_t = msghdr._6
    inline def msg_controllen_=(msg_controllen: size_t): Unit = !msghdr.at6 = msg_controllen
    inline def msg_flags: CInt = msghdr._7
    inline def msg_flags_=(msg_flags: CInt): Unit = !msghdr.at7 = msg_flags
  }

}
