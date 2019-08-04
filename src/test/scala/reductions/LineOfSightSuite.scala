package reductions

import java.util.concurrent._
import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory

@RunWith(classOf[JUnitRunner])
class LineOfSightSuite extends FunSuite {
  import LineOfSight._
  test("lineOfSight should correctly handle an array of size 4") {
    val output = new Array[Float](4)
    lineOfSight(Array[Float](0f, 1f, 8f, 9f), output)
    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

  test("upsweepSequential should correctly handle the chunk 1 until 4 of an array of 4 elements") {
    val res = upsweepSequential(Array[Float](0f, 1f, 8f, 9f), 1, 4)
    assert(res == 4f)
  }

  test("downsweepSequential should correctly handle a 4 element array when the starting angle is zero") {
    val output = new Array[Float](4)
    downsweepSequential(Array[Float](0f, 1f, 8f, 9f), output, 0f, 1, 4)
    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

  test("parallel threshold = 1") {
    val input = Array[Float](0.0f, 7.0f, 12.0f, 33.0f, 48.0f, 55.0f)
    val output = new Array[Float](input.length)
    val correctOutput = List(0.0f, 7.0f, 7.0f, 11.0f, 12.0f, 12.0f)

    LineOfSight.lineOfSight(input, output)
    assert(output.toList == correctOutput)

    val threshold = 1
    val parOutput = new Array[Float](input.length)
    LineOfSight.parLineOfSight(input, parOutput, threshold)
    assert(parOutput.toList == correctOutput)
  }

  test("parallel threshold = 2") {
    val input = Array[Float](0.0f, 7.0f, 12.0f, 33.0f, 48.0f)
    val output = new Array[Float](input.length)
    val correctOutput = List(0.0f, 7.0f, 7.0f, 11.0f, 12.0f)

    LineOfSight.lineOfSight(input, output)
    assert(output.toList == correctOutput)

    val threshold = 2
    val parOutput = new Array[Float](input.length)
    LineOfSight.parLineOfSight(input, parOutput, threshold)
    assert(parOutput.toList == correctOutput)
  }

  test("parallel threshold = input.length") {
    val input = Array[Float](0.0f, 7.0f, 12.0f, 33.0f, 48.0f, 55.0f)
    val output = new Array[Float](input.length)
    val correctOutput = List(0.0f, 7.0f, 7.0f, 11.0f, 12.0f, 12.0f)

    LineOfSight.lineOfSight(input, output)
    assert(output.toList == correctOutput)

    val threshold = input.length
    val parOutput = new Array[Float](input.length)
    LineOfSight.parLineOfSight(input, parOutput, threshold)
    assert(parOutput.toList == correctOutput)
  }

  test("downsweep should correctly handle an array when the starting angle is not zero") {
    val input = Array[Float](0.0f, 7.0f, 12.0f, 33.0f, 48.0f, 55.0f)
    val output = new Array[Float](input.length)
    val correctOutput = List(0.0f, 8.0f, 8.0f, 11.0f, 12.0f, 12.0f)

    val threshold = 1
    val tree = upsweep(input, 1, input.length, threshold)
    downsweep(input, output, 8.0f, tree)
    assert(output.toList == correctOutput)
  }

}
