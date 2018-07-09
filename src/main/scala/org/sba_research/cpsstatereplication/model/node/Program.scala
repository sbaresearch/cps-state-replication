package org.sba_research.cpsstatereplication.model.node

/**
  * A class for programs running on nodes.
  *
  * @param tags a map containing tags within the program: tagName -> type
  */
case class Program(tags: Map[String, String])
