package com.scala

import java.util.concurrent.ForkJoinPool

import scala.collection.parallel
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


object Boot extends App {

  // Group size is 3
  val groupSize = 3

  // And we need to get users from remote API by their ids
  val userIds: Seq[Int] = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

  case class User(id: Int, username: String)

  // Given we have some function which fetches the user from remote API
  def getUser(id: Int): Future[User] = Future {
    // Just mimic remote call
    Thread.sleep(1000)
    println(s"Getting user with id $id")
    User(id, s"user-$id")
  }

  val futures: Seq[() => Future[User]] = userIds.map(userId => () => getUser(userId))

  def runParallellyByGroups[T](eventualUsers: Seq[() => Future[T]], i: Int): Future[Seq[T]] = {

    def sliceIndexesTupleList(totalSize: Int, batchSize: Int): Seq[(Int, Int)] = {
      (0 until totalSize)
        .grouped(batchSize)
        .toSeq
        .map(sl => (sl.head, sl.last - sl.head + 1))
        .zipWithIndex
        .map(t => (t._1._1, t._1._2))
    }

    def parallelExtraction(startIndex: Int, batchSize: Int): Future[IndexedSeq[T]] = {
      val parallelList = eventualUsers
        .slice(startIndex, startIndex + batchSize)
        .par
      val fjpool = new ForkJoinPool(i)
      val customTaskSupport = new parallel.ForkJoinTaskSupport(fjpool)
      parallelList.tasksupport = customTaskSupport
      val result = parallelList.map(_.apply())

      Future.sequence(result.toIndexedSeq)
    }

    val usersBatch = sliceIndexesTupleList(eventualUsers.size, i)

    usersBatch
      .foldLeft(Future.successful(Seq.empty[T])) {
        (processedBatch, sliceIndexes) =>
          processedBatch
            .flatMap {
              userList =>
                val batchStartIndex = sliceIndexes._1
                val groupSize = sliceIndexes._2
                parallelExtraction(batchStartIndex, groupSize)
                  .map(u => {
                    u.toIndexedSeq ++ userList
                  })
            }
      }
  }

  val users: Future[Seq[User]] = runParallellyByGroups[User](futures, groupSize)

  Await.result(users, Duration.Inf)

  // Q: Please explain why runParallellyByGroups accepts Seq[() => Future[T]] but not just Seq[Future[T]]

  // A:
  // The signature of method getUser(id) starts with Future {...}, which is actually - def Future.apply[T](body: => T)
  // By-name parameter, is evaluated every time it is used

}
