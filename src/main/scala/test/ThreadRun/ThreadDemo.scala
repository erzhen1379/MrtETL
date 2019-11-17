package test.ThreadRun

/**
  * scala线程测试
  *
  */
class ThreadExample extends Thread {
  override def run(): Unit = {
    println("执行running方法")
  }
}

object ThreadDemo {
  def main(args: Array[String]): Unit = {
    var t1 = new ThreadExample()
    var t2 = new ThreadExample()
    t1.start()
    t2.start()

  }
}
