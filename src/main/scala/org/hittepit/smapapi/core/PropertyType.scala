package org.hittepit.smapapi.core

import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Types
import org.hittepit.smapapi.mapper.NullValueException
import java.sql.Clob
import java.sql.Blob

object Sql {
  type ColumnGetter[T] = ((ResultSet, Int) => T, (ResultSet, String) => T)
  type ColumnSetter[T] = ((PreparedStatement, Int, T) => Unit, Int)

  def getNullableColumn[T](getter: ColumnGetter[T])(rs: ResultSet, colonne: Either[String, Int]): Option[T] = {
    val v = colonne match {
      case Left(c) => getter._2(rs, c)
      case Right(c) => getter._1(rs, c)
    }
    if (rs.wasNull()) None else Some(v)
  }

  def getNotNullableColumn[T](getter: ColumnGetter[T])(rs: ResultSet, colonne: Either[String, Int]): T = {
    val v = colonne match {
      case Left(c) => getter._2(rs, c)
      case Right(c) => getter._1(rs, c)
    }
    if (rs.wasNull()) throw new NullValueException else v
  }

  def setNullableColumn[T](setter: ColumnSetter[T])(index: Int, value: Option[T], ps: PreparedStatement) = value match {
    case Some(v) => setter._1(ps, index, v)
    case None => ps.setNull(index, setter._2)
  }

  def setNotNullableColumn[T](setter: ColumnSetter[T])(index: Int, value: T, ps: PreparedStatement) =
    setter._1(ps, index, value)

  //TODO Long, Float, Date, Time
  val getStringColumn: ColumnGetter[String] = ((rs: ResultSet, index: Int) => rs.getString(index), (rs: ResultSet, name: String) => rs.getString(name))
  val setStringColumn: ColumnSetter[String] = ((ps: PreparedStatement, index: Int, v: String) => ps.setString(index, v), Types.VARCHAR)
  val getClobColumn: ColumnGetter[Clob] = ((rs: ResultSet, index: Int) => rs.getClob(index), (rs: ResultSet, name: String) => rs.getClob(name))
  val setClobColumn: ColumnSetter[Clob] = ((ps: PreparedStatement, index: Int, v: Clob) => ps.setClob(index, v), Types.CLOB)
  val getBlobColumn: ColumnGetter[Blob] = ((rs: ResultSet, index: Int) => rs.getBlob(index), (rs: ResultSet, name: String) => rs.getBlob(name))
  val setBlobColumn: ColumnSetter[Blob] = ((ps: PreparedStatement, index: Int, v: Blob) => ps.setBlob(index, v), Types.BLOB)
  val getIntColumn: ColumnGetter[Int] = ((rs: ResultSet, index: Int) => rs.getInt(index), (rs: ResultSet, name: String) => rs.getInt(name))
  val setIntColumn: ColumnSetter[Int] = ((ps: PreparedStatement, index: Int, v: Int) => ps.setInt(index, v), Types.INTEGER)
  val getDoubleColumn: ColumnGetter[Double] = ((rs: ResultSet, index: Int) => rs.getDouble(index), (rs: ResultSet, name: String) => rs.getDouble(name))
  val setDoubleColumn: ColumnSetter[Double] = ((ps: PreparedStatement, index: Int, v: Double) => ps.setDouble(index, v), Types.DOUBLE)
  val getBooleanColumn: ColumnGetter[Boolean] = ((rs: ResultSet, index: Int) => rs.getBoolean(index), (rs: ResultSet, name: String) => rs.getBoolean(name))
  val setBooleanColumn: ColumnSetter[Boolean] = ((ps: PreparedStatement, index: Int, v: Boolean) => ps.setBoolean(index, v), Types.BOOLEAN)
  val getBigDecimalColumn: ColumnGetter[BigDecimal] = ((rs: ResultSet, index: Int) => rs.getBigDecimal(index), (rs: ResultSet, name: String) => rs.getBigDecimal(name))
  val setBigDecimalColumn: ColumnSetter[BigDecimal] = ((ps: PreparedStatement, index: Int, v: BigDecimal) => ps.setBigDecimal(index, v.bigDecimal), Types.DECIMAL)

}

import Sql._

trait PropertyType[T] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]): T
  def setColumnValue(index: Int, value: T, ps: PreparedStatement): Unit
}

object NullableString extends PropertyType[Option[String]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getStringColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[String], ps: PreparedStatement) = setNullableColumn(setStringColumn)(index, value, ps)
}

object NotNullableString extends PropertyType[String] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getStringColumn)(rs, column)
  def setColumnValue(index: Int, value: String, ps: PreparedStatement) = setNotNullableColumn(setStringColumn)(index, value, ps)
}

object NullableClob extends PropertyType[Option[Clob]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getClobColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Clob], ps: PreparedStatement) = setNullableColumn(setClobColumn)(index, value, ps)
}

object NotNullableClob extends PropertyType[Clob] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getClobColumn)(rs, column)
  def setColumnValue(index: Int, value: Clob, ps: PreparedStatement) = setNotNullableColumn(setClobColumn)(index, value, ps)
}

object NullableBlob extends PropertyType[Option[Blob]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBlobColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Blob], ps: PreparedStatement) = setNullableColumn(setBlobColumn)(index, value, ps)
}

object NotNullableBlob extends PropertyType[Blob] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBlobColumn)(rs, column)
  def setColumnValue(index: Int, value: Blob, ps: PreparedStatement) = setNotNullableColumn(setBlobColumn)(index, value, ps)
}

object NullableInt extends PropertyType[Option[Int]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getIntColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Int], ps: PreparedStatement) = setNullableColumn(setIntColumn)(index, value, ps)
}

object NotNullableInt extends PropertyType[Int] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getIntColumn)(rs, column)
  def setColumnValue(index: Int, value: Int, ps: PreparedStatement) = setNotNullableColumn(setIntColumn)(index, value, ps)
}

object NullableDouble extends PropertyType[Option[Double]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getDoubleColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Double], ps: PreparedStatement) = setNullableColumn(setDoubleColumn)(index, value, ps)
}

object NotNullableDouble extends PropertyType[Double] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getDoubleColumn)(rs, column)
  def setColumnValue(index: Int, value: Double, ps: PreparedStatement) = setNotNullableColumn(setDoubleColumn)(index, value, ps)
}

object NullableBoolean extends PropertyType[Option[Boolean]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBooleanColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[Boolean], ps: PreparedStatement) = setNullableColumn(setBooleanColumn)(index, value, ps)
}

object NotNullableBoolean extends PropertyType[Boolean] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBooleanColumn)(rs, column)
  def setColumnValue(index: Int, value: Boolean, ps: PreparedStatement) = setNotNullableColumn(setBooleanColumn)(index, value, ps)
}

object NullableBigDecimal extends PropertyType[Option[BigDecimal]] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNullableColumn(getBigDecimalColumn)(rs, column)
  def setColumnValue(index: Int, value: Option[BigDecimal], ps: PreparedStatement) = setNullableColumn(setBigDecimalColumn)(index, value, ps)
}

object NotNullableBigDecimal extends PropertyType[BigDecimal] {
  def getColumnValue(rs: ResultSet, column: Either[String, Int]) = getNotNullableColumn(getBigDecimalColumn)(rs, column)
  def setColumnValue(index: Int, value: BigDecimal, ps: PreparedStatement) = setNotNullableColumn(setBigDecimalColumn)(index, value, ps)
}
