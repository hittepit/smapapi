package org.hittepit.smapapi.mapper

import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Types
import scala.language.higherKinds
import scala.util.Try
import scala.util.Success
import scala.util.Failure
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

  val getStringColumn: ColumnGetter[String] = ((rs: ResultSet, index: Int) => rs.getString(index), (rs: ResultSet, name: String) => rs.getString(name))
  val setStringColumn: ColumnSetter[String] = ((ps: PreparedStatement, index: Int, v: String) => ps.setString(index, v), Types.VARCHAR)
  val getIntColumn: ColumnGetter[Int] = ((rs: ResultSet, index: Int) => rs.getInt(index), (rs: ResultSet, name: String) => rs.getInt(name))
  val setIntColumn: ColumnSetter[Int] = ((ps: PreparedStatement, index: Int, v: Int) => ps.setInt(index, v), Types.INTEGER)
  val getDoubleColumn: ColumnGetter[Double] = ((rs: ResultSet, index: Int) => rs.getDouble(index), (rs: ResultSet, name: String) => rs.getDouble(name))
  val setDoubleColumn: ColumnSetter[Double] = ((ps: PreparedStatement, index: Int, v: Double) => ps.setDouble(index, v), Types.DOUBLE)

}

trait SqlType[T] extends Sql {
  val typeSql: Int
  val getColumnValue: (ResultSet, Either[String,Int]) => T
  val setColumnValue: (Int, T,PreparedStatement) => Unit
}

object NullableVarchar extends SqlType[Option[String]] {
  val typeSql = Types.VARCHAR
  val getColumnValue = getNullableColumn(getStringColumn)(_, _)
  val setColumnValue = setNullableColumn(setStringColumn)(_, _, _)
}

object NotNullableVarchar extends SqlType[String] {
  val typeSql = Types.VARCHAR
  val getColumnValue = getNotNullableColumn(getStringColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setStringColumn)(_, _, _)
}

object NullableInteger extends SqlType[Option[Int]] {
  val typeSql = Types.INTEGER
  val getColumnValue = getNullableColumn(getIntColumn)(_, _)
  val setColumnValue = setNullableColumn(setIntColumn)(_, _, _)
}

object NotNullableInteger extends SqlType[Int] {
  val typeSql = Types.INTEGER
  val getColumnValue = getNotNullableColumn(getIntColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setIntColumn)(_, _, _)
}

object NullableDouble extends SqlType[Option[Double]] {
  val typeSql = Types.INTEGER
  val getColumnValue = getNullableColumn(getDoubleColumn)(_, _)
  val setColumnValue = setNullableColumn(setDoubleColumn)(_, _, _)
}

object NotNullableDouble extends SqlType[Double] {
  val typeSql = Types.INTEGER
  val getColumnValue = getNotNullableColumn(getDoubleColumn)(_, _)
  val setColumnValue = setNotNullableColumn(setDoubleColumn)(_, _, _)
}

//trait SqlType[T]{
//  def columnValue(column:Either[String,Int])(implicit rs: ResultSet):T
//  def setParameter(index:Int,value:T)(implicit ps:PreparedStatement):Unit
//}
//
//trait Base [T] {
//  def get(name:String)(implicit rs:ResultSet):T
//  def get(index:Int)(implicit rs:ResultSet):T
// 
//  final def getColumnValue(column:Either[String,Int])(implicit rs:ResultSet):Try[T] = try{
//    val v = column match {
//    	case Left(c) => get(c)
//    	case Right(c) => get(c)
//    }
//    if(rs.wasNull) Failure(new NullValueException) else Success(v)
//  } catch {
//    case (e:Throwable) => Failure(e)
//  }
//  def setColumnValue(index:Int,value:T)(implicit ps:PreparedStatement):Unit
//  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement):Unit
//}
//
//trait Nullable[T] extends SqlType[Option[T]] with Base[T]{
//  def columnValue(column:Either[String,Int])(implicit rs: ResultSet) = getColumnValue(column) match{
//    case Success(v) => Some(v)
//    case Failure(e:NullValueException) => None
//    case Failure(e:Throwable) => throw e
//  }
//
//  def setParameter(index:Int,value:Option[T])(implicit ps:PreparedStatement):Unit = value match {
//    case Some(s) => setColumnValue(index, s)
//    case _ => setColumnNullValue(index)
//  }
//  
//}
//
//trait NotNullable[T] extends SqlType[T] with Base[T]{
//  def columnValue(columnName:Either[String,Int])(implicit rs: ResultSet)= getColumnValue(columnName) match {
//    case Success(v) => v
//    case Failure(e:Throwable) => throw e
//  }
//  
//  def setParameter(index:Int,value:T)(implicit ps:PreparedStatement):Unit = setColumnValue(index, value)
//}
//
//trait VarcharSqlType extends Base[String]{
//  def get(s:String)(implicit rs:ResultSet) = rs.getString(s)
//  def get(s:Int)(implicit rs:ResultSet) = rs.getString(s)
//
//  def setColumnValue(index:Int,value:String)(implicit ps:PreparedStatement) = ps.setString(index,value)
//  
//  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.VARCHAR)
//}
//
//object NullableVarchar extends Nullable[String] with VarcharSqlType
//object NotNullableVarchar extends NotNullable[String] with VarcharSqlType
//
//trait IntegerSqlType extends Base[Int]{
//  def get(s:String)(implicit rs:ResultSet) = rs.getInt(s)
//  def get(s:Int)(implicit rs:ResultSet) = rs.getInt(s)
//
//  def setColumnValue(index:Int,value:Int)(implicit ps:PreparedStatement) = ps.setInt(index,value)
//  
//  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.INTEGER)
//}
//
//object NotNullableInteger extends NotNullable[Int] with IntegerSqlType
//
//object NullableInteger extends Nullable[Int] with IntegerSqlType
//
//trait DoubleSqlType extends Base[Double]{
//  def get(s:String)(implicit rs:ResultSet) = rs.getDouble(s)
//  def get(s:Int)(implicit rs:ResultSet) = rs.getDouble(s)
//
//  def setColumnValue(index:Int,value:Double)(implicit ps:PreparedStatement) = ps.setDouble(index,value)
//  
//  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.DOUBLE)
//}
//
//object NullableDouble extends Nullable[Double] with DoubleSqlType
//object NotNullableDouble extends NotNullable[Double] with DoubleSqlType