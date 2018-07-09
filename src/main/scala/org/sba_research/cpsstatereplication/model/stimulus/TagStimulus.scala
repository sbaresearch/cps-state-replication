package org.sba_research.cpsstatereplication.model.stimulus

/**
  * Class representing a tag stimulus, i.e., the root of subsequent state transitions.
  *
  * @param timestamp the timestamp of the stimulus
  * @param twinName  the twin's name
  * @param tagName   the tag's name
  * @param value     optionally, the value to set (if no value is present, a get command will be issued)
  */
case class TagStimulus(timestamp: String, twinName: String, tagName: String, value: Option[String]) extends Stimulus
