package com.lossycounting

import scala.collection.mutable

class LossyCountingModel[T](
                             val frequency: Double,
                             val error: Double
                           ) extends FrequencyCount[T] {

  private var totalProcessedElements = 0L

  private val map = mutable.HashMap.empty[T, Int]


  def process(dataWindow: List[T]): LossyCountingModel[T] = {
    dataWindow.foreach { item =>
      totalProcessedElements += 1
      incrCount(item)
    }
    decreaseAllFrequencies()
    this
  }

  def computeOutput(): Array[(T, Int)] = {
    map.filter { itemWithFreq =>
      itemWithFreq._2.toDouble >= (frequency * totalProcessedElements - error * totalProcessedElements)
    }.toArray.sortWith((pair1, pair2) => pair1._2 > pair2._2)
  }

  def incrCount(item: T): Unit = {
    map.get(item) match {
      case Some(value) =>
        map.put(item, value + 1)
      case None =>
        map.put(item, 1)
    }
  }


  def decreaseAllFrequencies(): Unit = {
    map.foreach { itemFrequency =>
      if (itemShouldBeRemoved(itemFrequency)) {
        map.remove(itemFrequency._1)
      } else {
        map.put(itemFrequency._1, itemFrequency._2 - 1)
      }
    }
  }

  def itemShouldBeRemoved(itemFrequency: (T, Int)): Boolean = {
    itemFrequency._2 == 1
  }

}

