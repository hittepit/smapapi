package org.hittepit.smapapi.mapper

class Projection(properties:Seq[(Option[String],ColumnDefinition[_,_])]){
  def sqlString = properties.map{_ match {
	    case (None,column) => column.name
	    case (Some(alias),column) => column.name+" as "+alias
	  }}.mkString(",")
}

object Projection {
  def apply(properties:(Option[String],ColumnDefinition[_,_])*) = new Projection(properties)
}

