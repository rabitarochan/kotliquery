package kotliquery

import org.joda.time.DateTime
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Date
import java.time.*
import java.util.*
import kotlin.reflect.KProperty1
import kotlin.text.Regex

/**
 * Represents ResultSet and its each row.
 */
data class Row(
        val underlying: ResultSet,
        val cursor: Int = 0) : Sequence<Row> {

    private class RowIterator(val rs: ResultSet, val position: Int) : Iterator<Row> {
        override fun next(): Row {
            return Row(rs, position + 1)
        }

        override fun hasNext(): Boolean {
            return rs.isClosed == false && rs.next();
        }
    }

    override fun iterator(): Iterator<Row> {
        return RowIterator(underlying, cursor)
    }

    /**
     * Surely fetches nullable value from ResultSet.
     */
    private fun <A> nullable(v: A): A? {
        return if (underlying.wasNull()) null else v
    }

    fun statement(): Statement? {
        return nullable(underlying.statement)
    }

    fun warnings(): SQLWarning? {
        return underlying.warnings
    }

    fun next(): Boolean {
        return underlying.next()
    }

    fun close(): Unit {
        return underlying.close()
    }

    fun string(columnIndex: Int): String? {
        return nullable(underlying.getString(columnIndex))
    }

    fun string(columnLabel: String): String? {
        return nullable(underlying.getString(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, String?>): String? {
        val columnLabel = camelToSnake(prop.name)
        return string(columnLabel)
    }

    fun any(columnIndex: Int): Any? {
        return nullable(underlying.getObject(columnIndex))
    }

    fun any(columnLabel: String): Any? {
        return nullable(underlying.getObject(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Any?>): Any? {
        val columnLabel = camelToSnake(prop.name)
        return any(columnLabel)
    }

    fun long(columnIndex: Int): Long? {
        return nullable(underlying.getLong(columnIndex))
    }

    fun long(columnLabel: String): Long? {
        return nullable(underlying.getLong(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Long?>): Long? {
        val columnLabel = camelToSnake(prop.name)
        return long(columnLabel)
    }

    fun bytes(columnIndex: Int): ByteArray? {
        return nullable(underlying.getBytes(columnIndex))
    }

    fun bytes(columnLabel: String): ByteArray? {
        return nullable(underlying.getBytes(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, ByteArray?>): ByteArray? {
        val columnLabel = camelToSnake(prop.name)
        return bytes(columnLabel)
    }

    fun float(columnIndex: Int): Float? {
        return nullable(underlying.getFloat(columnIndex))
    }

    fun float(columnLabel: String): Float? {
        return nullable(underlying.getFloat(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Float?>): Float? {
        val columnLabel = camelToSnake(prop.name)
        return float(columnLabel)
    }

    fun short(columnIndex: Int): Short? {
        return nullable(underlying.getShort(columnIndex))
    }

    fun short(columnLabel: String): Short? {
        return nullable(underlying.getShort(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Short?>): Short? {
        val columnLabel = camelToSnake(prop.name)
        return short(columnLabel)
    }

    fun double(columnIndex: Int): Double? {
        return nullable(underlying.getDouble(columnIndex))
    }

    fun double(columnLabel: String): Double? {
        return nullable(underlying.getDouble(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Double?>): Double? {
        val columnLabel = camelToSnake(prop.name)
        return double(columnLabel)
    }

    fun int(columnIndex: Int): Int? {
        return nullable(underlying.getInt(columnIndex))
    }

    fun int(columnLabel: String): Int? {
        return nullable(underlying.getInt(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Int?>): Int? {
        val columnLabel = camelToSnake(prop.name)
        return int(columnLabel)
    }

    fun jodaDateTime(columnIndex: Int): DateTime? {
        val timestamp = sqlTimestamp(columnIndex)
        if (timestamp == null) {
            return null
        } else {
            return DateTime(timestamp)
        }
    }

    fun jodaDateTime(columnLabel: String): DateTime? {
        val timestamp = sqlTimestamp(columnLabel)
        if (timestamp == null) {
            return null
        } else {
            return DateTime(timestamp)
        }
    }

    fun <T> get(prop: KProperty1<T, DateTime?>): DateTime? {
        val columnLabel = camelToSnake(prop.name)
        return jodaDateTime(columnLabel)
    }

    fun jodaLocalDate(columnIndex: Int): org.joda.time.LocalDate? {
        val timestamp = sqlTimestamp(columnIndex)
        if (timestamp == null) {
            return null
        } else {
            return DateTime(timestamp).toLocalDate()
        }
    }

    fun jodaLocalDate(columnLabel: String): org.joda.time.LocalDate? {
        val timestamp = sqlTimestamp(columnLabel)
        if (timestamp == null) {
            return null
        } else {
            return DateTime(timestamp).toLocalDate()
        }
    }

    fun <T> get(prop: KProperty1<T, org.joda.time.LocalDate?>): org.joda.time.LocalDate? {
        val columnLabel = camelToSnake(prop.name)
        return jodaLocalDate(columnLabel)
    }

    fun jodaLocalTime(columnIndex: Int): org.joda.time.LocalTime? {
        val timestamp = sqlTimestamp(columnIndex)
        if (timestamp == null) {
            return null
        } else {
            return DateTime(timestamp).toLocalTime()
        }
    }

    fun jodaLocalTime(columnLabel: String): org.joda.time.LocalTime? {
        val timestamp = sqlTimestamp(columnLabel)
        if (timestamp == null) {
            return null
        } else {
            return DateTime(timestamp).toLocalTime()
        }
    }

    fun <T> get(prop: KProperty1<T, org.joda.time.LocalTime?>): org.joda.time.LocalTime? {
        val columnLabel = camelToSnake(prop.name)
        return jodaLocalTime(columnLabel)
    }

    fun zonedDateTime(columnIndex: Int): ZonedDateTime? {
        return nullable(ZonedDateTime.ofInstant(sqlTimestamp(columnIndex)?.toInstant(), ZoneId.systemDefault()))
    }

    fun zonedDateTime(columnLabel: String): ZonedDateTime? {
        return nullable(ZonedDateTime.ofInstant(sqlTimestamp(columnLabel)?.toInstant(), ZoneId.systemDefault()))
    }

    fun <T> get(prop: KProperty1<T, ZonedDateTime?>): ZonedDateTime? {
        val columnLabel = camelToSnake(prop.name)
        return zonedDateTime(columnLabel)
    }

    fun offsetDateTime(columnIndex: Int): OffsetDateTime? {
        return nullable(OffsetDateTime.ofInstant(sqlTimestamp(columnIndex)?.toInstant(), ZoneId.systemDefault()))
    }

    fun offsetDateTime(columnLabel: String): OffsetDateTime? {
        return nullable(OffsetDateTime.ofInstant(sqlTimestamp(columnLabel)?.toInstant(), ZoneId.systemDefault()))
    }

    fun <T> get(prop: KProperty1<T, OffsetDateTime?>): OffsetDateTime? {
        val columnLabel = camelToSnake(prop.name)
        return offsetDateTime(columnLabel)
    }

    fun instant(columnIndex: Int): Instant? {
        return nullable(sqlTimestamp(columnIndex)?.toInstant())
    }

    fun instant(columnLabel: String): Instant? {
        return nullable(sqlTimestamp(columnLabel)?.toInstant())
    }

    fun <T> get(prop: KProperty1<T, Instant?>): Instant? {
        val columnLabel = camelToSnake(prop.name)
        return instant(columnLabel)
    }

    fun localDateTime(columnIndex: Int): LocalDateTime? {
        return sqlTimestamp(columnIndex)?.toLocalDateTime()
    }

    fun localDateTime(columnLabel: String): LocalDateTime? {
        return sqlTimestamp(columnLabel)?.toLocalDateTime()
    }

    fun <T> get(prop: KProperty1<T, LocalDateTime?>): LocalDateTime? {
        val columnLabel = camelToSnake(prop.name)
        return localDateTime(columnLabel)
    }

    fun localDate(columnIndex: Int): LocalDate? {
        return sqlTimestamp(columnIndex)?.toLocalDateTime()?.toLocalDate()
    }

    fun localDate(columnLabel: String): LocalDate? {
        return sqlTimestamp(columnLabel)?.toLocalDateTime()?.toLocalDate()
    }

    fun <T> get(prop: KProperty1<T, LocalDate?>): LocalDate? {
        val columnLabel = camelToSnake(prop.name)
        return localDate(columnLabel)
    }

    fun localTime(columnIndex: Int): LocalTime? {
        return sqlTimestamp(columnIndex)?.toLocalDateTime()?.toLocalTime()
    }

    fun localTime(columnLabel: String): LocalTime? {
        return sqlTimestamp(columnLabel)?.toLocalDateTime()?.toLocalTime()
    }

    fun <T> get(prop: KProperty1<T, LocalTime?>): LocalTime? {
        val columnLabel = camelToSnake(prop.name)
        return localTime(columnLabel)
    }

    fun sqlDate(columnIndex: Int): java.sql.Date? {
        return nullable(underlying.getDate(columnIndex))
    }

    fun sqlDate(columnLabel: String): java.sql.Date? {
        return nullable(underlying.getDate(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, java.sql.Date?>): java.sql.Date? {
        val columnLabel = camelToSnake(prop.name)
        return sqlDate(columnLabel)
    }

    fun sqlDate(columnIndex: Int, cal: Calendar): Date? {
        return nullable(underlying.getDate(columnIndex, cal))
    }

    fun sqlDate(columnLabel: String, cal: Calendar): Date? {
        return nullable(underlying.getDate(columnLabel, cal))
    }

    fun <T> get(prop: KProperty1<T, Date?>, cal: Calendar): Date? {
        val columnLabel = camelToSnake(prop.name)
        return sqlDate(columnLabel, cal)
    }

    fun boolean(columnIndex: Int): Boolean {
        return underlying.getBoolean(columnIndex)
    }

    fun boolean(columnLabel: String): Boolean {
        return underlying.getBoolean(columnLabel)
    }

    fun <T> get(prop: KProperty1<T, Boolean?>): Boolean? {
        val columnLabel = camelToSnake(prop.name)
        return boolean(columnLabel)
    }

    fun bigDecimal(columnIndex: Int): BigDecimal? {
        return nullable(underlying.getBigDecimal(columnIndex))
    }

    fun bigDecimal(columnLabel: String): BigDecimal? {
        return nullable(underlying.getBigDecimal(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, BigDecimal?>): BigDecimal? {
        val columnLabel = camelToSnake(prop.name)
        return bigDecimal(columnLabel)
    }

    fun sqlTime(columnIndex: Int): java.sql.Time? {
        return nullable(underlying.getTime(columnIndex))
    }

    fun sqlTime(columnLabel: String): java.sql.Time? {
        return nullable(underlying.getTime(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, java.sql.Time?>): java.sql.Time? {
        val columnLabel = camelToSnake(prop.name)
        return sqlTime(columnLabel)
    }

    fun sqlTime(columnIndex: Int, cal: Calendar?): java.sql.Time? {
        return nullable(underlying.getTime(columnIndex, cal))
    }

    fun sqlTime(columnLabel: String, cal: Calendar?): java.sql.Time? {
        return nullable(underlying.getTime(columnLabel, cal))
    }

    fun <T> get(prop: KProperty1<T, java.sql.Time?>, cal: Calendar?): java.sql.Time? {
        val columnLabel = camelToSnake(prop.name)
        return sqlTime(columnLabel, cal)
    }

    fun url(columnIndex: Int): URL? {
        return nullable(underlying.getURL(columnIndex))
    }

    fun url(columnLabel: String): URL? {
        return nullable(underlying.getURL(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, URL?>): URL? {
        val columnLabel = camelToSnake(prop.name)
        return url(columnLabel)
    }

    fun blob(columnIndex: Int): Blob? {
        return nullable(underlying.getBlob(columnIndex))
    }

    fun blob(columnLabel: String): Blob? {
        return nullable(underlying.getBlob(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Blob?>): Blob? {
        val columnLabel = camelToSnake(prop.name)
        return blob(columnLabel)
    }

    fun byte(columnIndex: Int): Byte? {
        return nullable(underlying.getByte(columnIndex))
    }

    fun byte(columnLabel: String): Byte? {
        return nullable(underlying.getByte(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Byte?>): Byte? {
        val columnLabel = camelToSnake(prop.name)
        return byte(columnLabel)
    }

    fun clob(columnIndex: Int): java.sql.Clob? {
        return nullable(underlying.getClob(columnIndex))
    }

    fun clob(columnLabel: String): java.sql.Clob? {
        return nullable(underlying.getClob(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, java.sql.Clob?>): java.sql.Clob? {
        val columnLabel = camelToSnake(prop.name)
        return clob(columnLabel)
    }

    fun nClob(columnIndex: Int): NClob? {
        return nullable(underlying.getNClob(columnIndex))
    }

    fun nClob(columnLabel: String): NClob? {
        return nullable(underlying.getNClob(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, NClob?>): NClob? {
        val columnLabel = camelToSnake(prop.name)
        return nClob(columnLabel)
    }

    fun sqlArray(columnIndex: Int): java.sql.Array? {
        return nullable(underlying.getArray(columnIndex))
    }

    fun sqlArray(columnLabel: String): java.sql.Array? {
        return nullable(underlying.getArray(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, java.sql.Array?>): java.sql.Array? {
        val columnLabel = camelToSnake(prop.name)
        return sqlArray(columnLabel)
    }

    fun asciiStream(columnIndex: Int): InputStream? {
        return nullable(underlying.getAsciiStream(columnIndex))
    }

    fun asciiStream(columnLabel: String): InputStream? {
        return nullable(underlying.getAsciiStream(columnLabel))
    }

    fun <T> getAsciiStream(prop: KProperty1<T, InputStream?>): InputStream? {
        val columnLabel = camelToSnake(prop.name)
        return asciiStream(columnLabel)
    }

    fun sqlTimestamp(columnIndex: Int): java.sql.Timestamp? {
        return nullable(underlying.getTimestamp(columnIndex))
    }

    fun sqlTimestamp(columnLabel: String): java.sql.Timestamp? {
        return nullable(underlying.getTimestamp(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, java.sql.Timestamp?>): java.sql.Timestamp? {
        val columnLabel = camelToSnake(prop.name)
        return sqlTimestamp(columnLabel)
    }

    fun sqlTimestamp(columnIndex: Int, cal: Calendar): java.sql.Timestamp? {
        return nullable(underlying.getTimestamp(columnIndex, cal))
    }

    fun sqlTimestamp(columnLabel: String, cal: Calendar): java.sql.Timestamp? {
        return nullable(underlying.getTimestamp(columnLabel, cal))
    }

    fun <T> get(prop: KProperty1<T, java.sql.Timestamp?>, cal: Calendar): java.sql.Timestamp? {
        val columnLabel = camelToSnake(prop.name)
        return sqlTimestamp(columnLabel, cal)
    }

    fun ref(columnIndex: Int): Ref? {
        return nullable(underlying.getRef(columnIndex))
    }

    fun ref(columnLabel: String): Ref? {
        return nullable(underlying.getRef(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Ref?>): Ref? {
        val columnLabel = camelToSnake(prop.name)
        return ref(columnLabel)
    }

    fun nCharacterStream(columnIndex: Int): Reader? {
        return nullable(underlying.getNCharacterStream(columnIndex))
    }

    fun nCharacterStream(columnLabel: String): Reader? {
        return nullable(underlying.getNCharacterStream(columnLabel))
    }

    fun <T> get(prop: KProperty1<T, Reader?>): Reader? {
        val columnLabel = camelToSnake(prop.name)
        return nCharacterStream(columnLabel)
    }

    fun metaData(): ResultSetMetaData {
        return underlying.metaData
    }

    fun binaryStream(columnIndex: Int): InputStream? {
        return nullable(underlying.getBinaryStream(columnIndex))
    }

    fun binaryStream(columnLabel: String): InputStream? {
        return nullable(underlying.getBinaryStream(columnLabel))
    }

    fun <T> getBinaryStream(prop: KProperty1<T, InputStream?>): InputStream? {
        val columnLabel = camelToSnake(prop.name)
        return binaryStream(columnLabel)
    }

    private fun camelToSnake(name: String): String {
        val snake = name.replace(Regex("([A-Z]+)([A-Z][a-z])"), "$1_$2")
                        .replace(Regex("([a-z])([A-Z])"), "$1_$2")
        return snake.toLowerCase()
    }

}