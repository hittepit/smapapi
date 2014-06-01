package org.hittepit.smapapi.mapper

import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Types
import scala.language.higherKinds
import scala.util.Try
import scala.util.Success
import scala.util.Failure

trait SqlType[T]{
  def columnValue(column:Either[String,Int])(implicit rs: ResultSet):T
  def setParameter(index:Int,value:T)(implicit ps:PreparedStatement):Unit
}

trait Base [T] {
  def get(name:String)(implicit rs:ResultSet):T
  def get(index:Int)(implicit rs:ResultSet):T
 
  final def getColumnValue(column:Either[String,Int])(implicit rs:ResultSet):Try[T] = try{
    val v = column match {
    	case Left(c) => get(c)
    	case Right(c) => get(c)
    }
    if(rs.wasNull) Failure(new NullValueException) else Success(v)
  } catch {
    case (e:Throwable) => Failure(e)
  }
  def setColumnValue(index:Int,value:T)(implicit ps:PreparedStatement):Unit
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement):Unit
}

trait Nullable[T] extends SqlType[Option[T]] with Base[T]{
  def columnValue(column:Either[String,Int])(implicit rs: ResultSet) = getColumnValue(column) match{
    case Success(v) => Some(v)
    case Failure(e:NullValueException) => None
    case Failure(e:Throwable) => throw e
  }

  def setParameter(index:Int,value:Option[T])(implicit ps:PreparedStatement):Unit = value match {
    case Some(s) => setColumnValue(index, s)
    case _ => setColumnNullValue(index)
  }
  
}

trait NotNullable[T] extends SqlType[T] with Base[T]{
  def columnValue(columnName:Either[String,Int])(implicit rs: ResultSet)= getColumnValue(columnName) match {
    case Success(v) => v
    case Failure(e:Throwable) => throw e
  }
  
  def setParameter(index:Int,value:T)(implicit ps:PreparedStatement):Unit = setColumnValue(index, value)
}

trait VarcharSqlType extends Base[String]{
  def get(s:String)(implicit rs:ResultSet) = rs.getString(s)
  def get(s:Int)(implicit rs:ResultSet) = rs.getString(s)

  def setColumnValue(index:Int,value:String)(implicit ps:PreparedStatement) = ps.setString(index,value)
  
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.VARCHAR)
}

object NullableVarchar extends Nullable[String] with VarcharSqlType
object NotNullableVarchar extends NotNullable[String] with VarcharSqlType

trait IntegerSqlType extends Base[Int]{
  def get(s:String)(implicit rs:ResultSet) = rs.getInt(s)
  def get(s:Int)(implicit rs:ResultSet) = rs.getInt(s)

  def setColumnValue(index:Int,value:Int)(implicit ps:PreparedStatement) = ps.setInt(index,value)
  
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.INTEGER)
}

object NotNullableInteger extends NotNullable[Int] with IntegerSqlType

object NullableInteger extends Nullable[Int] with IntegerSqlType

trait DoubleSqlType extends Base[Double]{
  def get(s:String)(implicit rs:ResultSet) = rs.getDouble(s)
  def get(s:Int)(implicit rs:ResultSet) = rs.getDouble(s)

  def setColumnValue(index:Int,value:Double)(implicit ps:PreparedStatement) = ps.setDouble(index,value)
  
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.DOUBLE)
}

object NullableDouble extends Nullable[Double] with DoubleSqlType
object NotNullableDouble extends NotNullable[Double] with DoubleSqlType