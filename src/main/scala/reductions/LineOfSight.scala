package reductions

import org.scalameter._
import common._

import scala.util.Random

object LineOfSightRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 100,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]) {
    val length = 10000000
    val input = (0 until length).map(_ % 100 * 1.0f).toArray
    val threshold = 10000
    val output = new Array[Float](length + 1)

    val seqtime = standardConfig measure {
      LineOfSight.lineOfSight(input, output)
    }
    println(s"sequential time: $seqtime ms")
    println(s"sequential input: ${input.zipWithIndex.map(p => p._1 / p._2).toList}")
    println(s"sequential output: ${output.toList}")

    val partime = standardConfig measure {
      LineOfSight.parLineOfSight(input, output, threshold)
    }
    println(s"parallel time: $partime ms")
    println(s"parallel input: ${input.zipWithIndex.map(p => p._1 / p._2).toList}")
    println(s"parallel output: ${output.toList}")
    println(s"speedup: ${seqtime / partime}")
  }
}

object LineOfSight {

  def max(a: Float, b: Float): Float = if (a > b) a else b

  def lineOfSight(input: Array[Float], output: Array[Float]): Unit = {
    var maxHeight = 0F
    output(0) = maxHeight
    var i = 1
    while (i < input.length) {
      maxHeight = max(maxHeight, input(i) / i)
      output(i) = maxHeight
      i += 1
    }
  }

  sealed abstract class Tree {
    def maxPrevious: Float
  }

  case class Node(left: Tree, right: Tree) extends Tree {
    val maxPrevious: Float = max(left.maxPrevious, right.maxPrevious)
  }

  case class Leaf(from: Int, until: Int, maxPrevious: Float) extends Tree

  /** Traverses the specified part of the array and returns the maximum angle.
    */
  def upsweepSequential(input: Array[Float], from: Int, until: Int): Float = {
    var maxHeight = 0F
    var i = from
    while (i < until) {
      maxHeight = max(maxHeight, input(i) / i)
      i += 1
    }
    maxHeight
  }

  /** Traverses the part of the array starting at `newFrom` and until `end`, and
    *  returns the reduction tree for that part of the array.
    *
    *  The reduction tree is a `Leaf` if the length of the specified part of the
    *  array is smaller or equal to `threshold`, and a `Node` otherwise.
    *  If the specified part of the array is longer than `threshold`, then the
    *  work is divided and done recursively in parallel.
    */
  def upsweep(input: Array[Float], from: Int, end: Int, threshold: Int): Tree = {
    if (end - from <= threshold) {
      Leaf(from, end, upsweepSequential(input, from, end))
    } else {
      val mid = from + (end - from) / 2
      val (lTree, rTree) = parallel(upsweep(input, from, mid, threshold), upsweep(input, mid, end, threshold))
      Node(lTree, rTree)
    }
  }

  /** Traverses the part of the `input` array starting at `newFrom` and until
    *  `until`, and computes the maximum angle for each entry of the output array,
    *  given the `startingAngle`.
    */
  def downsweepSequential(input: Array[Float],
                          output: Array[Float],
                          startingAngle: Float,
                          from: Int,
                          until: Int): Unit = {
    var maxHeight = startingAngle
    var i = from
    while (i < until) {
      maxHeight = max(maxHeight, input(i) / i)
      output(i) = maxHeight
      i += 1
    }
  }

  /** Pushes the maximum angle in the prefix of the array to each leaf of the
    *  reduction `tree` in parallel, and then calls `downsweepSequential` to write
    *  the `output` angles.
    */
  def downsweep(input: Array[Float], output: Array[Float], startingAngle: Float, tree: Tree): Unit = {
    tree match {
      case Node(left, right) =>
        val maxLeft = if (startingAngle == 0) left.maxPrevious else max(startingAngle, left.maxPrevious)
        val maxRight = if (startingAngle == 0) left.maxPrevious else max(startingAngle, left.maxPrevious)
        parallel(
          downsweep(input, output, maxLeft, left),
          downsweep(input, output, maxRight, right)
        )
      case Leaf(from, until, maxPrevious) =>
        if (from == 1 || until - from != 1) {
          downsweepSequential(input, output, startingAngle, from, until)
        } else {
          downsweepSequential(input, output, max(startingAngle, maxPrevious), from, until)
        }
    }
  }

  /** Compute the line-of-sight in parallel. */
  def parLineOfSight(input: Array[Float], output: Array[Float], threshold: Int): Unit = {
    val tree = upsweep(input, 1, input.length, threshold)
    output(0) = 0
    downsweep(input, output, 0, tree)
  }
}
