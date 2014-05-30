package org.hittepit.smapapi.mapper

import java.sql.PreparedStatement

trait Condition {
	def sqlString():String
	def setParameter(index:Int,ps:PreparedStatement):Int
}

class PropertyCondition[P](c:ColumnDefinition[_,P],value:P) extends Condition {
	def sqlString() = c.name+"=?"
	
	def setParameter(index:Int, ps:PreparedStatement) = {
	  c.sqlType.setParameter(index+1, value)(ps)
	  index+1
	}
}

class AndCondition(conditions:Condition*) extends Condition {
  def sqlString() = conditions match {
    case Seq(c1) => c1.sqlString()
    case Seq(c1,cs@_*) => (c1.sqlString()/:cs)((currentSql,c)=>" and "+c.sqlString())
    case Nil => throw new Exception
  }
  
  def setParameter(index:Int,ps:PreparedStatement):Int=2
} 