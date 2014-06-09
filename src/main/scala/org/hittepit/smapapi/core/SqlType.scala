package org.hittepit.smapapi.core

import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Types
import org.hittepit.smapapi.mapper.NullValueException
import java.sql.Clob
import java.sql.Blob
trait Sql {
  type ColumnGetter[T] = ((ResultSet, Int) => T, (ResultSet, String) => T)
  type ColumnSetter[T] = ((PreparedStatement, Int, T) => Unit, Int)

  def getNullableColumn[T](getter: ColumnGetter[T])(rs: ResultSet, colonne: Either[String,Int]): Option[T] = {
    val v = colonne match {
      case Left(c) => getter._2(rs, c)
      case Right(c) => getter._1(rs, c)
    }
    if (rs.wasNull()) None else Some(v)
  }
  
  def getNotNullableColumn[T](getter: ColumnGetter[T])(rs: ResultSet, colonne: Either[String,Int]): T = {
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
  val getBigDecimalColumn:ColumnGetter[BigDecimal] = ((rs: ResultSet, index:Int) => rs.getBigDecimal(index), (rs: ResultSet, name: String) => rs.getBigDecimal(name))
  val setBigDecimalColumn: ColumnSetter[BigDecimal] = ((ps: PreparedStatement, index: Int, v: BigDecimal) => ps.setBigDecimal(index, v.bigDecimal), Types.DECIMAL)
  
}

trait SqlType[T] extends Sql {
//  val typeSql: Int
  val getColumnValue: (ResultSet, Either[String,Int]) => T
  val setColumnValue: (Int, T,PreparedStatement) => Unit
}

object NullableVarchar extends SqlType[Option[String]] {
//  val typeSql = Types.VARCHAR
  val getColumnValue = getNullableColumn(getStringColumn)(_, _)
  val setColumnValue = setNullableColumn(setStringColumn)(_, _, _)
}

object NotNullableVarchar extends SqlType[String] {
//  val typeSql = Types.VARCHAR
  val getColumnValue = getNotNullableColumn(getStringColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setStringColumn)(_, _, _)
}

object NullableClob extends SqlType[Option[Clob]] {
  val getColumnValue = getNullableColumn(getClobColumn)(_, _)
  val setColumnValue = setNullableColumn(setClobColumn)(_, _, _)
}

object NotNullableClob extends SqlType[Clob] {
  val getColumnValue = getNotNullableColumn(getClobColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setClobColumn)(_, _, _)
}

object NullableBlob extends SqlType[Option[Blob]] {
  val getColumnValue = getNullableColumn(getBlobColumn)(_, _)
  val setColumnValue = setNullableColumn(setBlobColumn)(_, _, _)
}

object NotNullableBlob extends SqlType[Blob] {
  val getColumnValue = getNotNullableColumn(getBlobColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setBlobColumn)(_, _, _)
}

object NullableInteger extends SqlType[Option[Int]] {
//  val typeSql = Types.INTEGER
  val getColumnValue = getNullableColumn(getIntColumn)(_, _)
  val setColumnValue = setNullableColumn(setIntColumn)(_, _, _)
}

object NotNullableInteger extends SqlType[Int] {
//  val typeSql = Types.INTEGER
  val getColumnValue = getNotNullableColumn(getIntColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setIntColumn)(_, _, _)
}

object NullableDouble extends SqlType[Option[Double]] {
//  val typeSql = Types.INTEGER
  val getColumnValue = getNullableColumn(getDoubleColumn)(_, _)
  val setColumnValue = setNullableColumn(setDoubleColumn)(_, _, _)
}

object NotNullableDouble extends SqlType[Double] {
//  val typeSql = Types.INTEGER
  val getColumnValue = getNotNullableColumn(getDoubleColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setDoubleColumn)(_, _, _)
}

object NullableBoolean extends SqlType[Option[Boolean]] {
  val getColumnValue = getNullableColumn(getBooleanColumn)(_, _)
  val setColumnValue = setNullableColumn(setBooleanColumn)(_, _, _)
}

object NotNullableBoolean extends SqlType[Boolean] {
  val getColumnValue = getNotNullableColumn(getBooleanColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setBooleanColumn)(_, _, _)
}

object NullableBigDecimal extends SqlType[Option[BigDecimal]] {
//  val typeSql = Types.DECIMAL
  val getColumnValue = getNullableColumn(getBigDecimalColumn)(_, _)
  val setColumnValue = setNullableColumn(setBigDecimalColumn)(_, _, _)
}

object NotNullableBigDecimal extends SqlType[BigDecimal] {
//  val typeSql = Types.DECIMAL
  val getColumnValue = getNotNullableColumn(getBigDecimalColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setBigDecimalColumn)(_, _, _)
}
