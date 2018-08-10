package com.ovoenergy.fs2.kafka

import cats.effect.{Async, Sync, Concurrent}
import cats.effect.implicits._
import cats.syntax.traverse._
import cats.syntax.functor._
import cats.syntax.either._
import com.ovoenergy.fs2.kafka.Producing._
import fs2._
import fs2.async.mutable.Topic
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._

/**
  * The Producing side of the Kafka client.
  */
trait Producing {

  /**
    * Provides a `Pipe[F, ProducerRecord[K, V], RecordMetadata]` that will send each record to kafka.
    */
  def produce[F[_]]: ProducePartiallyApplied[F] =
    new ProducePartiallyApplied[F]

  /**
    * Provides a `Stream[F, Producer[K,V]]` that will automatically close the producer when completed.
    */
  def producerStream[F[_]]: ProducerStreamPartiallyApplied[F] =
    new ProducerStreamPartiallyApplied[F]

  /**
    * Sends a ProducerRecord[K,V] to Kafka.
    */
  def produceRecord[F[_]]: ProduceRecordPartiallyApplied[F] =
    new ProduceRecordPartiallyApplied[F]

  /**
    * Sends a ProducerRecord[K,V] to Kafka. It returns an F[F[RecordMetadata]], the outer F represent the effect to put
    * the record in the batch. The inner F represent the effect to send the record to Kafka broker.
    *
    * The reason for that is allow the Kafka producer to optimize the network communication by sending records in
    * batches instead of one record at time.
    */
  def produceRecordWithBatching[F[_]]
    : ProduceRecordWithBatchingPartiallyApplied[F] =
    new ProduceRecordWithBatchingPartiallyApplied[F]()

  /**
    * Processes a `Chunk[(ProducerRecord[K, V], P)]`, sending the records to Kafka
    * in the order they are provided. The passthrough values of type `P` are left
    * as is in the output.
    */
  def produceRecordBatch[F[_]]: ProduceRecordBatchPartiallyApplied[F] =
    new ProduceRecordBatchPartiallyApplied[F]()

  /**
    * Sends items, communicated through `fs2.Topic` as ProducerRecord[K,V] to Kafka.
    */
  def subscribedProduce[F[_]]: SubscribedProducePartiallyApplied[F] =
    new SubscribedProducePartiallyApplied[F]

}

object Producing {

  private[kafka] final class ProducePartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](settings: ProducerSettings,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V])(
        implicit F: Async[F]): Pipe[F, ProducerRecord[K, V], RecordMetadata] = {

      record: Stream[F, ProducerRecord[K, V]] =>
        producerStream(settings, keySerializer, valueSerializer).repeat
          .zip(record)
          .evalMap {
            case (p, pr) =>
              produceRecord(p, pr)
          }

    }
  }

  private[kafka] final class ProducerStreamPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](settings: ProducerSettings,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V])(
        implicit F: Sync[F]): Stream[F, Producer[K, V]] = {

      Stream
        .bracket(
          initProducer[F, K, V](settings.nativeSettings,
                                keySerializer,
                                valueSerializer))(p => Sync[F].delay(p.close()))

    }
  }

  private[kafka] final class ProduceRecordPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](producer: Producer[K, V], record: ProducerRecord[K, V])(
        implicit F: Async[F]): F[RecordMetadata] = {

      F.async[RecordMetadata] { cb =>
        producer.send(
          record,
          (metadata: RecordMetadata, exception: Exception) =>
            Option(exception) match {
              case Some(e) => cb(Left(KafkaProduceException(record, e)))
              case None    => cb(Right(metadata))
          }
        )

        ()
      }
    }
  }

  private[kafka] final class ProduceRecordWithBatchingPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](producer: Producer[K, V], record: ProducerRecord[K, V])(
        implicit F: Concurrent[F]): F[F[RecordMetadata]] = {

      def send: F[RecordMetadata] = F.async { cb =>
        producer.send(
          record,
          new Callback {
            override def onCompletion(metadata: RecordMetadata,
                                      exception: Exception): Unit =
              cb(Option(exception).toLeft(metadata))
          }
        )
        ()
      }

      send.start.map(_.join)
    }
  }

  private[kafka] final class ProduceRecordBatchPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V, P](producer: Producer[K, V],
                       recordBatch: Chunk[(ProducerRecord[K, V], P)])(
        implicit F: Async[F]): F[Chunk[(RecordMetadata, P)]] = {
      def getMetadata(record: ProducerRecord[K, V],
                      p: P): F[(RecordMetadata, P)] =
        F.async { cb =>
          producer.send(
            record,
            (metadata: RecordMetadata, exception: Exception) =>
              Option(exception) match {
                case Some(e) => cb(KafkaProduceException(record, e).asLeft)
                case None    => cb((metadata, p).asRight)
            }
          )
          ()
        }

      recordBatch.traverse { case (r, p) => getMetadata(r, p) }
    }
    //   F.delay j
    //   F.flatten(F.delay {
    //     recordBatch.traverse {
    //       // TODO change to async only
    //       case (record, p) =>
    //         val promise = scala.concurrent.Promise[(RecordMetadata, P)]()

    //         producer.send(
    //           record,
    //           (metadata: RecordMetadata, exception: Exception) =>
    //             Option(exception) match {
    //               case Some(e) =>
    //                 promise.complete(Failure(KafkaProduceException(record, e)));
    //                 ()
    //               case None => promise.complete(Success((metadata, p))); ()
    //           }
    //         )

    //         F.async { cb =>
    //           promise.future.onComplete {
    //             case Success(result)    => cb(Right(result))
    //             case Failure(exception) => cb(Left(exception))
    //           }
    //         }
    //     }
    //   })
    // }
  }

  private[kafka] final class SubscribedProducePartiallyApplied[F[_]] {
    def apply[K, V, B](
        p: Producer[K, V],
        topic: Topic[F, B],
        transformer: Pipe[F, B, ProducerRecord[K, V]],
        maxQueueSize: Int = 500
    )(implicit F: Async[F]): Stream[F, RecordMetadata] =
      topic
        .subscribe(maxQueueSize)
        .through(transformer)
        .evalMap(pr => produceRecord(p, pr))
  }

  private def initProducer[F[_]: Sync, K, V](
      nativeSettings: Map[String, AnyRef],
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]): F[Producer[K, V]] = Sync[F].delay {
    new KafkaProducer[K, V](nativeSettings.asJava,
                            keySerializer,
                            valueSerializer)
  }

}
