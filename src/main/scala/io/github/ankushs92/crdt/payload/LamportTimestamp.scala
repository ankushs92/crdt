package io.github.ankushs92.crdt.payload

import java.util.concurrent.atomic.AtomicLong

class LamportTimestamp  {

    private val counter = new AtomicLong(0L)

    def increment = counter.set(counter.get + 1)

    def state = counter.get

    def setNew(value : Long) = counter.set(value)

    def lessThan(lamportTimestamp: LamportTimestamp) : Boolean = this.state < lamportTimestamp.state

    override def toString: String = state.toString

}
