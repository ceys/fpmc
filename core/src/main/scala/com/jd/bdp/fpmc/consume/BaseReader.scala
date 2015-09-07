package main.scala.com.jd.bdp.fpmc.consume


/**
 * Created by zhengchen on 2015/9/7.
 */
abstract class BaseReader[R, E] extends Serializable{

  def makeExamples(r: R): E

}
