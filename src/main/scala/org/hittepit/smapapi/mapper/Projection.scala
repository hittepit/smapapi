package org.hittepit.smapapi.mapper

trait Projection{
  def sqlString:String
}

class PropertyProjection(property: (Option[String],ColumnName)) extends Projection{
  def sqlString = property._1 match {
    case None => property._2.name
    case Some(alias) => property._2.name+" as "+alias
  }
}

class ProjectionList(projections:Seq[Projection]) extends Projection{
  def sqlString = projections.map(_.sqlString).mkString(",")
}

object Projection {
  def apply(properties:(Option[String],ColumnDefinition[_,_])*) = new ProjectionList(properties.map{new PropertyProjection(_)}) 
}

