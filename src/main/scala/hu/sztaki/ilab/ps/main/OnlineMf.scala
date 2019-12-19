package hu.sztaki.ilab.ps.main

object OnlineMf {


  def main(args: Array[String]): Unit = {
    val model = new FlinkKafkaPsWrapper
    model.runMf _ tupled model.parseProperties(args)
  }

}