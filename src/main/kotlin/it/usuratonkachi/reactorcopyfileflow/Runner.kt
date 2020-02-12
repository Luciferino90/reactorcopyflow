package it.usuratonkachi.reactorcopyfileflow

import org.reactivestreams.Publisher
import org.springframework.boot.CommandLineRunner
import org.springframework.core.io.buffer.DataBuffer
import org.springframework.core.io.buffer.DataBufferUtils
import org.springframework.core.io.buffer.DefaultDataBufferFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*


@Component
class KotlindemoRunner : CommandLineRunner {

    private val sizeBuffer = 1024 * 32
    private val sizeTot = 1024 * 1024 * 1024
    private val random = Random()
    private val dataBufferFactory = DefaultDataBufferFactory()

    private var writtenSize = 0

    var handler: CompletionHandler<Int, String?> = object : CompletionHandler<Int, String?> {
        override fun completed(result: Int?, attachment: String?) {
            println(attachment.toString() + " completed and " + result + " bytes are written.")
        }

        override fun failed(exc: Throwable, attachment: String?) {
            println(attachment.toString() + " failed with exception:")
            exc.printStackTrace()
        }
    }

    override fun run(vararg args: String?) {
        val start = Date()
        println("Application Started @ $start")
        //val pathToFileSrc = "/Users/Luca/Documents/test/test.txt"
        val pathToFileSrc = "/Users/Luca/Documents/test/test_cdb6f4e1-a080-41a5-ac3b-17ba13bcd68f.txt"
        val pathToFileDest = pathToFileSrc.split(".")[0] + "_" + UUID.randomUUID().toString() + "." + pathToFileSrc.split(".")[1]
        val pathSrc = Paths.get(pathToFileSrc)
        val pathDest = Paths.get(pathToFileDest)
        //generateFile(start, pathDest)
        //copyFileReactive(pathSrc, pathDest, sizeBuffer, start)
        copyFileImperative(pathSrc, pathDest, sizeBuffer, start)
        //Files.deleteIfExists(pathDest)
        System.exit(0)
    }

    fun generateFile(start: Date, pathDest: Path) {
        generateNewFileFile(pathDest, sizeBuffer, sizeTot)
        val end = Date()
        println("Application Ended @ $end")
        val diff = end.time.minus(start.time)
        val copiedBufferSize = writtenSize
        if (writtenSize == 0)
            println("$diff milli secondi")
        else {
            val copyRate = writtenSize.div(1024).div(1024).div(diff)
            println("Copiati $copiedBufferSize byte In $diff milli secondi, rate $copyRate MB/s")
        }
    }

    fun copyFileReactive(pathSrc: Path, pathDest: Path, sizeBuffer: Int, start: Date) {
        copyFileReactive(pathSrc, pathDest, sizeBuffer)
        val end = Date()
        println("Application Ended @ $end")
        val diff = end.time.minus(start.time)
        val copiedBufferSize = writtenSize
        if (writtenSize == 0)
            println("$diff milli secondi")
        else {
            val copyRate = writtenSize.div(1024).div(1024).div(diff)
            println("Copiati $copiedBufferSize byte In $diff milli secondi, rate $copyRate MB/s")
        }
    }

    fun copyFileImperative(pathSrc: Path, pathDest: Path, sizeBuffer: Int, start: Date) {
        Files.copy(pathSrc, pathDest)
        //writtenSize += Files.readAllBytes(pathSrc).size
        val end = Date()
        println("Application Ended @ $end")
        val diff = end.time.minus(start.time)
        val copiedBufferSize = writtenSize
        if (writtenSize == 0)
            println("$diff milli secondi")
        else {
            val copyRate = writtenSize.div(1024).div(1024).div(diff)
            println("Copiati $copiedBufferSize byte In $diff milli secondi, rate $copyRate MB/s")
        }
    }

    fun copyFileReactive(pathSrc: Path, pathDest: Path, sizeBuffer: Int) {
        val fileChannelRead = getAsyncFileChannel(pathSrc, StandardOpenOption.READ)
        val fileChannelWrite = getAsyncFileChannel(pathDest, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val bytes = ByteArray(sizeBuffer)
        random.nextBytes(bytes)
        val dataRead : Flux<DataBuffer> = DataBufferUtils.readAsynchronousFileChannel({ fileChannelRead }, 0, dataBufferFactory, sizeBuffer)
        DataBufferUtils.write(dataRead, fileChannelWrite)
                //.doOnNext { writtenSize += it.readableByteCount() }
                .collectList().block()
        fileChannelRead.close()
        fileChannelWrite.close()
    }

    fun generateNewFileFile(pathDest: Path, sizeBuffer: Int, sizeTot: Int) {
        var fileChannelWrite = getAsyncFileChannel(pathDest, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)

        DataBufferUtils.write(multipleChunks(sizeBuffer, sizeTot),fileChannelWrite ).collectList()
                .map { e -> {
                    closeChannel(fileChannelWrite)
                } }
                .block()
    }

    fun multipleChunks(sizeBuffer: Int, sizeTot: Int) : Publisher<DataBuffer> {
        val chunkRepetition : Int = sizeTot / sizeBuffer
        return Flux.range(1, chunkRepetition).doOnNext { writtenSize += sizeBuffer }.map { randomBuffer(sizeBuffer) }
    }

    fun randomBuffer(size: Int): DataBuffer {
        val bytes = ByteArray(size)
        random.nextBytes(bytes)
        val buffer = dataBufferFactory.allocateBuffer(size)
        buffer.write(bytes)
        return buffer
    }

    fun getAsyncFileChannel(path: Path, vararg  openOption: StandardOpenOption?) : AsynchronousFileChannel {
        return AsynchronousFileChannel.open(path, *openOption)
    }

    fun closeChannel(channel: AsynchronousFileChannel) {
        channel.close()
    }

}
