package org.hittepit.smapapi.mapper

import java.sql.ResultSet
import java.sql.PreparedStatement
import java.sql.Types

trait SqlType[T] {
  protected def getValueFromResultSet(columnName: String)(implicit rs: ResultSet): T
  
  def columnValue(columnName: String)(implicit rs: ResultSet) = {
    val v = getValueFromResultSet(columnName)
    if (rs.wasNull) throw new NullValueException else v
  }
  
  def setParameter(index:Int, ps:PreparedStatement, value:Option[_]):Unit
}

object Integer extends SqlType[Int] {
  def getValueFromResultSet(columnName: String)(implicit rs: ResultSet) = rs.getInt(columnName)

  def setParameter(index:Int, ps:PreparedStatement, value:Option[_]){
    value match {
      case Some(v:Int) => ps.setInt(index,v)
      case Some(_) => throw new Exception("Pas le bon type de paramètre") //TODO
      case None => ps.setNull(index, Types.INTEGER)
    }
  }
}

object Varchar extends SqlType[String] {
  def getValueFromResultSet(columnName: String)(implicit rs: ResultSet) = rs.getString(columnName)

  def setParameter(index:Int, ps:PreparedStatement, value:Option[_]){
    value match {
      case Some(v:String) => ps.setString(index,v)
      case Some(_) => throw new Exception("Pas le bon type de paramètre") //TODO
      case None => ps.setNull(index, Types.VARCHAR)
    }
  }
}

object Nullable {
  def apply[T](sqlType: SqlType[T]): SqlType[Option[T]] = new SqlType[Option[T]] {
    def getValueFromResultSet(columnName: String)(implicit rs: ResultSet): Option[T] = None
    
    def setParameter(index:Int, ps:PreparedStatement, value:Option[_]) = sqlType.setParameter(index, ps, value)
    
    override def columnValue(columnName: String)(implicit rs: ResultSet) = {
      try {
        Some(sqlType.columnValue(columnName))
      } catch {
        case NullValueException() => None
        case e: Throwable => throw e
      }
    }
  }
  
}

object NotNullable {
  def apply[T](sqlType: SqlType[T]): SqlType[T] = sqlType
}

class ColumnDefinition[T, S, U](n: String, st: SqlType[S], g: T => U, pk:Boolean) {
	val name = n
	val sqlType = st
	val getter = g
	val primaryKey = pk
	
//  def convert(sqlValue: S): U = sqlValue.asInstanceOf[U]

  def value(t: T): U = getter(t)

  def value(implicit rs: ResultSet) = sqlType.columnValue(name)
  
//  def format(v:Any):String = sqlType.formatSql(v)
}
