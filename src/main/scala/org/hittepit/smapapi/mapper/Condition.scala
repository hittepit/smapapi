package org.hittepit.smapapi.mapper

import java.sql.PreparedStatement

/*
Oracle
1 	Unary + - arithmetic operators, PRIOR operator
2 	* / arithmetic operators
3 	Binary + - arithmetic operators, || character operators
4 	All comparison operators
5 	NOT logical operator
6 	AND logical operator
7 	OR logical operator 
*/

trait Condition {
	val precedence:Int
	def sqlString():String
	def setParameter(index:Int,ps:PreparedStatement):Int
}

object Condition{
  def eq[P](c:ColumnDefinition[_,P],value:P) = new EqualsCondition(c,value)
  def ne[P](c:ColumnDefinition[_,P],value:P):Condition = new NotEqualsCondition(c,value)
  def gt[P](c:ColumnDefinition[_,P],value:P):Condition = new GreaterThanCondition(c,value)
  def ge[P](c:ColumnDefinition[_,P],value:P):Condition = new GreaterOrEqualsCondition(c,value)
  def lt[P](c:ColumnDefinition[_,P],value:P):Condition = new LowerThanCondition(c,value)
  def le[P](c:ColumnDefinition[_,P],value:P):Condition = new LowerOrEqualsCondition(c,value)
  def like[P](c:ColumnDefinition[_,P],value:P):Condition = new LikeCondition(c,value)
  def not(c:Condition):Condition = new NotCondition(c)
  def and(cs:Condition*):Condition = new AndCondition(cs)
  def or(cs:Condition*):Condition = new OrCondition(cs)
  def in[P](c:ColumnDefinition[_,P],values:P*):Condition = new InCondition(c,values)
  def isNull(c:ColumnDefinition[_,_]):Condition = new IsNullCondition(c)
  def isNotNull[P](c:ColumnDefinition[_,P]):Condition = new IsNotNullCondition(c)
  def between[P](c:ColumnDefinition[_,P],value1:P,value2:P):Condition = new BetweenCondition(c,value1,value2)
  def sql(sql:String, params:List[Param[_]]) = new SqlCondition(sql,params)
}

case class Param[T](value:T,sqlType:SqlType[T])

class SqlCondition(sql:String, params:List[Param[_]]) extends Condition {
  val precedence = 10
  
  def sqlString = sql
  
  def setParameter(index:Int, ps:PreparedStatement) = {
    var i = index
    params.foreach{_ match {
      case Param(value,sqlType) => sqlType.setColumnValue(i+1,value,ps)
    		  						i = i+1
    } }
    i
  }
}

class EqualsCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+"=?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setColumnValue(index+1, value,ps)
	  index+1
	}
}

class NotEqualsCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+"<>?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setColumnValue(index+1, value,ps)
	  index+1
	}
}

class LikeCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+" like ?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setColumnValue(index+1, value,ps)
	  index+1
	}
}

class GreaterThanCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+">?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setColumnValue(index+1, value,ps)
	  index+1
	}
}

class GreaterOrEqualsCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+">=?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setColumnValue(index+1, value,ps)
	  index+1
	}
}

class LowerThanCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+"<?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setColumnValue(index+1, value,ps)
	  index+1
	}
}

class LowerOrEqualsCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+"<=?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setColumnValue(index+1, value,ps)
	  index+1
	}
}

class BetweenCondition[P](c:ColumnDefinition[_,P],v1:P,v2:P) extends Condition {
  val precedence = 4
  
  def sqlString = c.name+" between ? and ?"
  
  def setParameter(index:Int, ps:PreparedStatement) = {
    c.sqlType.setColumnValue(index+1, v1,ps)
    c.sqlType.setColumnValue(index+2, v2,ps)
    index+2
  }
}

class InCondition[P](c:ColumnDefinition[_,P],values:Seq[P]) extends Condition{
  val precedence = 4
  
  def sqlString = c.name+" in ("+(List.fill(values.size)("?")).mkString(",")+")"
  
  def setParameter(index:Int, ps:PreparedStatement) = {
    var newIndex = index
    values.foreach{v => newIndex = newIndex+1
      				c.sqlType.setColumnValue(newIndex,v,ps)
      				}
    newIndex
  }
}

class IsNullCondition(c:ColumnDefinition[_,_]) extends Condition {
  val precedence =4
  
  def sqlString = c.name+" is null"
  
  def setParameter(index:Int,ps:PreparedStatement) = index
}

class IsNotNullCondition(c:ColumnDefinition[_,_]) extends Condition {
  val precedence =4
  
  def sqlString = c.name+" is not null"
  
  def setParameter(index:Int,ps:PreparedStatement) = index
}

trait Combination extends Condition{
  val conditions:Seq[Condition]
  val operator:String
	
  def sqlString() = conditions match {
    case Seq(c1) => c1.sqlString()
    case Seq(c1,cs@_*) => val start = if(c1.precedence > precedence) "("+c1.sqlString+")" else c1.sqlString 
      					(start/:cs)((currentSql,c)=>currentSql+" "+operator+" "+(if(c.precedence>precedence) "("+c.sqlString()+")" else c.sqlString))
    case Nil => throw new Exception
  }
  
  def setParameter(index:Int,ps:PreparedStatement):Int={
    var newIndex = index
    conditions.foreach{c => newIndex = c.setParameter(newIndex, ps)}
    newIndex
  }
}

class NotCondition(condition:Condition) extends Combination {
	val conditions = Seq(condition)
	val precedence = 5
	val operator = "not"
	  
	override def sqlString() = "not "+(if(condition.precedence > precedence) "("+condition.sqlString+")" else condition.sqlString)
}

class AndCondition(val conditions:Seq[Condition]) extends Combination {
	val precedence = 6
	val operator = "and"
} 


class OrCondition(val conditions:Seq[Condition]) extends Combination {
	val precedence = 7
	val operator = "or"
} 