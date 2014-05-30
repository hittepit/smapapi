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

class PropertyCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	val precedence = 4
	def sqlString() = c.name+"=?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setParameter(index+1, value)(ps)
	  index+1
	}
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

class AndCondition(val conditions:Condition*) extends Combination {
	val precedence = 6
	val operator = "and"
} 


class OrCondition(val conditions:Condition*) extends Combination {
	val precedence = 7
	val operator = "or"
} 