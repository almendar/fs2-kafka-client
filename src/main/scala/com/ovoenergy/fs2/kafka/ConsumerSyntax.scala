package com.ovoenergy.fs2.kafka

import cats.effect.{Sync, Concurrent}
import fs2._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization.Deserializer

import ConsumerSyntax._

/**
  * The Consuming side of the Kafka client.
  */
trait ConsumerSyntax {

  /**
    * Consume records from the given subscription and provides a `Stream[F, ConsumerRecord[K, V]]`.
    */
  def consume[F[_]]: ConsumePartiallyApplied[F] = new ConsumePartiallyApplied[F]

  /**
    * Consume records from the given subscription, and apply the provided function on each record,
    * and commit the offsets to Kafka. The records in each topic/partition will be processed in
    * sequence, while multiple topic/partitions will be processed in parallel, up to the
    * specified parallelism.
    *
    * The result of the processing is a `Stream[F, O]` where `O` is the
    * return type of the provided function.
    */
  def consumeProcessAndCommit[F[_]]
    : ConsumeProcessAndCommitPartiallyApplied[F] =
    new ConsumeProcessAndCommitPartiallyApplied[F]

  /**
    * Consume records from the given subscription, and apply the provided function on each batch
    * of records, and commit the offsets to Kafka. The records in each topic/partition will be
    * processed in sequence, while multiple topic/partitions will be processed in parallel,
    * up to the specified parallelism.
    *
    * The result of the processing is a `Stream[F, O]` where `O` is the
    * return type of the provided function.
    */
  def consumeProcessBatchAndCommit[F[_]]
    : ConsumeProcessBatchAndCommitPartiallyApplied[F] =
    new ConsumeProcessBatchAndCommitPartiallyApplied[F]

  /**
    * Consume records from the given subscription, and apply the provided `Pipe[F[_], ConsumerRecord[K, V], O]` function on each batch
    * of records which is converted to stream, and commit the offsets to Kafka after the stream terminated. Note that the resulting stream
    * can emit less or more elements compared to the input stream.
    * The complete error handling is delegated to the provided pipe. When the stream terminates it is assumed that every emitted element was processed
    * and the latest offset in the batch will be used for commit.
    * The records in each topic/partition will be processed in sequence, while multiple topic/partitions will be processed in parallel,
    * up to the specified parallelism.
    *
    * The result of the processing is a `Stream[F, BatchResults[O]]`.
    */
  def consumeProcessBatchWithPipeAndCommit[F[_]]
    : ConsumeProcessBatchWithPipeAndCommitPartiallyApplied[F] =
    new ConsumeProcessBatchWithPipeAndCommitPartiallyApplied[F]

  /**
    * Provides a `Stream[F, Consumer[K,V]]` that will automatically close the consumer when completed.
    */
  def consumerStream[F[_]]: ConsumerStreamPartiallyApplied[F] =
    new ConsumerStreamPartiallyApplied[F]

}

object ConsumerSyntax {

  private[kafka] final class ConsumePartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {

    def apply[K, V](subscription: Subscription,
                    keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    settings: ConsumerSettings)(
        implicit F: Sync[F]): Stream[F, ConsumerRecord[K, V]] =
      consuming.consume[F, K, V](subscription,
                                 keyDeserializer,
                                 valueDeserializer,
                                 settings)
  }

  private[kafka] final class ConsumeProcessAndCommitPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V, O](subscription: Subscription,
                       keyDeserializer: Deserializer[K],
                       valueDeserializer: Deserializer[V],
                       settings: ConsumerSettings)(
        processRecord: ConsumerRecord[K, V] => F[O])(
        implicit F: Concurrent[F]): Stream[F, O] =
      consuming.consumeProcessAndCommit[F, K, V, O](subscription,
                                                    keyDeserializer,
                                                    valueDeserializer,
                                                    settings)(processRecord)
  }

  private[kafka] final class ConsumeProcessBatchAndCommitPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V, O](subscription: Subscription,
                       keyDeserializer: Deserializer[K],
                       valueDeserializer: Deserializer[V],
                       settings: ConsumerSettings)(
        processRecordBatch: Chunk[ConsumerRecord[K, V]] => F[
          Chunk[(O, Offset)]])(implicit F: Concurrent[F]): Stream[F, O] =
      consuming.consumeProcessBatchAndCommit[F, K, V, O](
        subscription,
        keyDeserializer,
        valueDeserializer,
        settings)(processRecordBatch)
  }

  private[kafka] final class ConsumeProcessBatchWithPipeAndCommitPartiallyApplied[
      F[_]](val dummy: Boolean = true)
      extends AnyVal {
    def apply[K, V, O](subscription: Subscription,
                       keyDeserializer: Deserializer[K],
                       valueDeserializer: Deserializer[V],
                       settings: ConsumerSettings)(
        processRecordBatch: Pipe[F, ConsumerRecord[K, V], O])(
        implicit F: Concurrent[F]): Stream[F, BatchResults[O]] =
      consuming.consumeProcessBatchWithPipeAndCommit(
        subscription,
        keyDeserializer,
        valueDeserializer,
        settings)(processRecordBatch)
  }

  private[kafka] final class ConsumerStreamPartiallyApplied[F[_]](
      val dummy: Boolean = true)
      extends AnyVal {

    def apply[K, V](keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    settings: ConsumerSettings)(
        implicit F: Sync[F]): Stream[F, Consumer[K, V]] =
      consuming.consumerStream[F, K, V](keyDeserializer,
                                        valueDeserializer,
                                        settings)
  }
}
