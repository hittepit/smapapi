package org.hittepit.smapapi.mapper

import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Types
import scala.language.higherKinds

trait SqlType[T]{
  def columnValue(column:Either[String,Int])(implicit rs: ResultSet):T
  def setParameter(index:Int,value:T)(implicit ps:PreparedStatement):Unit
}

trait Base [T] {
  def getColumnValue(column:Either[String,Int])(implicit rs:ResultSet):T
  def setColumnValue(index:Int,value:T)(implicit ps:PreparedStatement):Unit
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement):Unit
}

trait Nullable[T] extends SqlType[Option[T]] with Base[T]{
  def columnValue(column:Either[String,Int])(implicit rs: ResultSet) = try{
    Some(getColumnValue(column))
  } catch {
    case e:NullValueException => None
  }

  def setParameter(index:Int,value:Option[T])(implicit ps:PreparedStatement):Unit = value match {
    case Some(s) => setColumnValue(index, s)
    case _ => setColumnNullValue(index)
  }
  
}

trait NotNullable[T] extends SqlType[T] with Base[T]{
  def columnValue(columnName:Either[String,Int])(implicit rs: ResultSet)=getColumnValue(columnName)
  
  def setParameter(index:Int,value:T)(implicit ps:PreparedStatement):Unit = setColumnValue(index, value)
}

trait VarcharSqlType extends Base[String]{
  def getColumnValue(column:Either[String,Int])(implicit rs:ResultSet) = {
    val v = column match {
    			case Left(columnName) =>rs.getString(columnName)
    			case Right(index) => rs.getString(index)
    		}
    if(rs.wasNull) throw new NullValueException else v
  }

  def setColumnValue(index:Int,value:String)(implicit ps:PreparedStatement) = ps.setString(index,value)
  
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.VARCHAR)
}

object NullableVarchar extends Nullable[String] with VarcharSqlType
object NotNullableVarchar extends NotNullable[String] with VarcharSqlType

trait IntegerSqlType extends Base[Int]{
  def getColumnValue(column:Either[String,Int])(implicit rs:ResultSet) = {
    val v = column match {
    			case Left(columnName) =>rs.getInt(columnName)
    			case Right(index) => rs.getInt(index)
    		}
    if(rs.wasNull) throw new NullValueException else v
  }

  def setColumnValue(index:Int,value:Int)(implicit ps:PreparedStatement) = ps.setInt(index,value)
  
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.INTEGER)
}

object NotNullableInteger extends NotNullable[Int] with IntegerSqlType

object NullableInteger extends Nullable[Int] with IntegerSqlType

trait DoubleSqlType extends Base[Double]{
  def getColumnValue(column:Either[String,Int])(implicit rs:ResultSet) = {
    val v = column match {
    			case Left(columnName) =>rs.getDouble(columnName)
    			case Right(index) => rs.getDouble(index)
    		}
    if(rs.wasNull) throw new NullValueException else v
  }

  def setColumnValue(index:Int,value:Double)(implicit ps:PreparedStatement) = ps.setDouble(index,value)
  
  def setColumnNullValue(index:Int)(implicit ps:PreparedStatement) = ps.setNull(index, Types.DOUBLE)
}

object NullableDouble extends Nullable[Double] with DoubleSqlType
object NotNullableDouble extends NotNullable[Double] with DoubleSqlType