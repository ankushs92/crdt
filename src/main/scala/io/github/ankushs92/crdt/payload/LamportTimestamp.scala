package io.github.ankushs92.crdt.payload

import java.util.concurrent.atomic.AtomicLong

class LamportTimestamp  {

    private val counter = new AtomicLong(0L)

    def increment = counter.set(counter.get + 1)

    def state = counter.get

    def setNew(value : Long) = counter.set(value)

    def lessThan(other: LamportTimestamp) : Boolean = this.state < other.state

    override def toString: String = state.toString

}

object t extends App {

}