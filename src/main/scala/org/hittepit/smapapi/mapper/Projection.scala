package org.hittepit.smapapi.mapper

trait Projection{
  def sqlString:String
}

class PropertyProjection(property: (Option[String],String)) extends Projection{
  def sqlString = property._1 match {
    case None => property._2
    case Some(alias) => property._2+" as "+alias
  }
}

class CountProjection(property:(Option[String],Option[String])) extends Projection{
  def sqlString = property match {
    case (alias,name) => "count("+ (name match{
      case Some(n) => n
      case None => "*"
    })+")"+(alias match {
      case Some(a) => " as "+a
      case None =>""
    })
  }
}

class ProjectionList(projections:Seq[Projection]) extends Projection{
  def sqlString = projections.map(_.sqlString).mkString(",")
}

object Projection {
  def apply(properties:(Option[String],ColumnName)*) = new ProjectionList(properties.map{tuple =>
    new PropertyProjection(tuple._1,tuple._2.name)
  }) 
}