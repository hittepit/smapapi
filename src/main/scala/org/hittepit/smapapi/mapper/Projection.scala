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

class CountProjection(property:Option[(Option[String],String)]) extends Projection{
  def sqlString = property match {
    case Some((alias,name)) => alias match {
      case Some(a) => "count("+name+") as "+alias
      case None => "count("+name+")"
    }
      
    case None => "count(*)"
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