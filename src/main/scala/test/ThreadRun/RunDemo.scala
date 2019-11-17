package test.ThreadRun

class RunExample extends Runnable{
  override def run(): Unit ={
    println("执行running方法")
  }
}



object RunDemo {
  def main(args: Array[String]): Unit = {
    var e=new RunExample()
    var t=new Thread(e)
    t.start()


  }
}
