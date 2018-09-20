package com.ovoenergy.fs2.kafka

import cats.effect.{Async, Sync, Concurrent}
import fs2._, concurrent.Topic
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

import ProducerSyntax._

/**
  * The Producing side of the Kafka client.
  */
trait ProducerSyntax {

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

object ProducerSyntax {

  private[kafka] final class ProducePartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](settings: ProducerSettings,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V])(
        implicit F: Async[F]): Pipe[F, ProducerRecord[K, V], RecordMetadata] =
      producing.produce[F, K, V](settings, keySerializer, valueSerializer)
  }

  private[kafka] final class ProducerStreamPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](settings: ProducerSettings,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V])(
        implicit F: Sync[F]): Stream[F, Producer[K, V]] =
      producing.producerStream[F, K, V](settings,
                                        keySerializer,
                                        valueSerializer)
  }

  private[kafka] final class ProduceRecordPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](producer: Producer[K, V], record: ProducerRecord[K, V])(
        implicit F: Async[F]): F[RecordMetadata] =
      producing.produceRecord[F, K, V](producer, record)
  }

  private[kafka] final class ProduceRecordWithBatchingPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V](producer: Producer[K, V], record: ProducerRecord[K, V])(
        implicit F: Concurrent[F]): F[F[RecordMetadata]] =
      producing.produceRecordWithBatching[F, K, V](producer, record)
  }

  private[kafka] final class ProduceRecordBatchPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V, P](producer: Producer[K, V],
                       recordBatch: Chunk[(ProducerRecord[K, V], P)])(
        implicit F: Async[F]): F[Chunk[(RecordMetadata, P)]] =
      producing.produceRecordBatch[F, K, V, P](producer, recordBatch)
  }

  private[kafka] final class SubscribedProducePartiallyApplied[F[_]] {
    def apply[K, V, B](
        p: Producer[K, V],
        topic: Topic[F, B],
        transformer: Pipe[F, B, ProducerRecord[K, V]],
        maxQueueSize: Int = 500
    )(implicit F: Async[F]): Stream[F, RecordMetadata] =
      producing.subscribedProduce[F, K, V, B](p,
                                              topic,
                                              transformer,
                                              maxQueueSize)
  }
}
