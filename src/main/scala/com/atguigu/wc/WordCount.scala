package com.atguigu.wc

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * @author
  * @create
  */
//批处理 word count程序
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建一个执行环境
    val env= ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath = "E:\\learn\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDateSet = env.readTextFile(inputPath)

    val stream1 = inputDateSet.flatMap(new myflatmap())
      .print()

//    //切分数据得到word,然后再按word做分组聚合
//    val wordCountDataSet = inputDateSet.flatMap(_.split(" "))
//        .map((_,1))
//        .groupBy(0)
//        .sum(1)
//
//    wordCountDataSet.print()
  }
}

class myflatmap() extends FlatMapFunction[String,String]{
  override def flatMap(value: String, out: Collector[String]): Unit = {
    val dataArray = value.split(" ")
    for (word <- dataArray){
      if (word.length == 4){
        out.collect(word)
      }
    }
  }
}