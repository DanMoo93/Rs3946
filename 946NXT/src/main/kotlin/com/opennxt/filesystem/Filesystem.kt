package com.opennxt.filesystem

import java.nio.ByteBuffer
import java.nio.file.Path

abstract class Filesystem(val path: Path) {

    private val checkedReferenceTables = BooleanArray(255)

    private val cachedReferenceTables = arrayOfNulls<ReferenceTable>(255)

    abstract fun exists(index: Int, archive: Int): Boolean

    abstract fun read(index: Int, archive: Int): ByteBuffer?

    abstract fun read(index: Int, name: String): ByteBuffer?

    abstract fun readReferenceTable(index: Int): ByteBuffer?

    abstract fun createIndex(id: Int)

    fun getReferenceTable(index: Int, ignoreChecked: Boolean = false): ReferenceTable? {
        val cached = cachedReferenceTables[index]
        if (cached != null) return cached

        if (!ignoreChecked) {
            if (checkedReferenceTables[index]) return null
            checkedReferenceTables[index] = true
        }

        val table = ReferenceTable(this, index)
        val container = readReferenceTable(index) ?: return null

        // After ZLB unwrapping, reference table data can be either:
        // 1. Standard Container-wrapped (first byte = compression type 0-3)
        // 2. Raw reference table (first byte = format version >= 5, e.g. 7 for 946)
        val firstByte = container.get(container.position()).toInt() and 0xff
        val data = if (firstByte <= 3) {
            ByteBuffer.wrap(Container.decode(container).data)
        } else {
            container // raw reference table data, not wrapped in Container
        }
        table.decode(data)
        cachedReferenceTables[index] = table

        return table
    }

    abstract fun write(index: Int, archive: Int, data: Container)

    abstract fun write(index: Int, archive: Int, compressed: ByteArray, version: Int, crc: Int)

    abstract fun writeReferenceTable(index: Int, data: Container)

    abstract fun writeReferenceTable(index: Int, compressed: ByteArray, version: Int, crc: Int)

    abstract fun numIndices(): Int

    fun update() {
        cachedReferenceTables.forEach { it?.update() }
    }

}