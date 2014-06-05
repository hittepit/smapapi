package org.hittepit.smapapi.core

import java.sql.Connection
import java.sql.ResultSet
import org.hittepit.smapapi.mapper.SqlType
import java.sql.ResultSetMetaData

case class Param[T](value:T,sqlType:SqlType[T])


class Row(resultSet:ResultSet,rsmd:ResultSetMetaData){
  def getColumnValue[T](index:Int,sqlType:SqlType[T]) = sqlType.getColumnValue(resultSet,Right(index))
  def getColumnValue[T](columnName:String,sqlType:SqlType[T]) = sqlType.getColumnValue(resultSet,Left(columnName))

}

class Session(val connection:Connection) {
	def streamRs(rsmd:ResultSetMetaData,rs:ResultSet):Stream[Row] = if(rs.next()) new Row(rs,rsmd) #:: streamRs(rsmd,rs) else Stream.Empty
	
	def select(sql:String,params:List[Param[_]]):Stream[Row] = {
	  val ps = connection.prepareStatement(sql)
	  
	  params.zipWithIndex.foreach{_ match{
	    case (param:Param[_],index) => param.sqlType.setColumnValue(index+1,param.value,ps)
	  }}
	  
	  streamRs(ps.getMetaData(),ps.executeQuery)
	}
	
	def select[T](sql:String,params:List[Param[_]],mapper:Row => T):Seq[T] = {
	  select(sql,params) map (mapper)
	}
}