package network.cere.ddc.client.consumer

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream

class ChunkStream : WriteStream<Buffer> {
    private var exceptionHandler: Handler<Throwable>? = null
    private var drainHandler: Handler<Void>? = null
    private val stream = UnicastProcessor.create<Buffer>()

    override fun exceptionHandler(handler: Handler<Throwable>?): WriteStream<Buffer> {
        exceptionHandler = handler
        return this
    }

    override fun write(data: Buffer?): Future<Void> {
        stream.onNext(data)
        return Future.succeededFuture()
    }

    override fun write(data: Buffer?, handler: Handler<AsyncResult<Void?>?>) {
        stream.onNext(data)
        handler.handle(Future.succeededFuture())
    }

    override fun end(handler: Handler<AsyncResult<Void?>?>) {
        stream.onComplete()
        handler.handle(Future.succeededFuture())
    }

    override fun setWriteQueueMaxSize(maxSize: Int): WriteStream<Buffer> {
        return this
    }

    override fun writeQueueFull(): Boolean {
        return false
    }

    override fun drainHandler(handler: Handler<Void>?): WriteStream<Buffer> {
        drainHandler = handler
        return this
    }

    fun toMulti(): Multi<Buffer> {
        return stream
    }
}
