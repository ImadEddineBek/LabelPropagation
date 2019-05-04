import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, GraphLoader, Pregel, VertexId}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import java.io.File
import java.io.PrintWriter

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val data_path = args(1)
    val spark = SparkSession
      .builder()
      .appName("Label Propagation")
      .config("spark.master", "local[2]")
      .getOrCreate()
    println("I am here" + data_path)
    // Create a graph from the csv file
    var gr = GraphLoader.edgeListFile(spark.sparkContext, data_path)
    val init_message = Map[Long, Long]()

    // The LPA

    val g = Pregel(
      graph = gr.mapVertices { case (vid, _) => vid.toLong }, // Every node is initialized with it's label
      initialMsg = init_message, // first message is an empty one
      maxIterations = 3 // just to run an example
      ,
    )(
      vprog = (id: Long, vd: Long, a: Map[Long, Long]) => {
        if (a.isEmpty) vd else a.maxBy(_._2)._1 // takes the max label or keeps it's own label
      },
      sendMsg = (e: EdgeTriplet[Long, Int]) => {

        Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L))) // send a message to with it's label to it's neighbours
      },
      mergeMsg = (a: Map[Long, Long], b: Map[Long, Long]) => {
        // if they have the same label then sum them up
        (a.keySet ++ b.keySet).map { i =>
          val count1Val = a.getOrElse(i, 0L)
          val count2Val = b.getOrElse(i, 0L)
          i -> (count1Val + count2Val)
        }(collection.breakOut)
      }
    )


    g.vertices.foreach(println)
    var writer = new PrintWriter(new File("clustering.txt"))
    g.vertices.collect().foreach(x => {
      writer.write(x._1 + " " + x._2 + "\n") // writing to a file the content of the vertices of the graph
    })
    writer.close()

    val stats = analyze(g)
    val max = stats._4
    val min = stats._3
    val mean = stats._1
    val variance = stats._2
    val values = arg_max_min(g, max, min)
    val minimum = values._2
    val maximum = values._1
    print(max, min, mean, variance)
    writer = new PrintWriter(new File("max.txt"))
    g.vertices.filter(x => x._2 == maximum).collect().foreach(x => { // writing to a file the content of the maximum
      // cluster of the graph
      writer.write(x._1 + " " + x._2 + "\n")
    })
    writer.close()

    writer = new PrintWriter(new File("min.txt"))
    g.vertices.filter(x => x._2 == minimum).collect().foreach(x => {// writing to a file the content of the minimum
      // cluster of the graph
      writer.write(x._1 + " " + x._2 + "\n")
    })
    writer.close()


  }

  /**
    * returns the mean number of elements in cluster, var,min, max.
    *
    * @param g : the nodes graph
    * @return mean, variance, min, max
    */
  def analyze(g: Graph[Long, Int]): (Double, Double, Int, Int) = {
    val nb_elements = g.vertices.groupBy(x => x._2).map(x => x._2.count(x => x == x))
    (nb_elements.mean(), nb_elements.variance(), nb_elements.min(), nb_elements.max())
  }

  /**
    *
    * return the labels of the max and min clusters
    *
    * @param g   : the nodes graph
    * @param max : the number of elements in the biggest cluster
    * @param min : the number of elements in the smallest cluster
    * @return maximum, minimum
    */
  def arg_max_min(g: Graph[Long, Int], max: Int, min: Int): (VertexId, VertexId) = {
    val elements = g.vertices.groupBy(x => x._2).map(x => (x._1, x._2.count(x => x == x)))
    val maximum = elements.filter(x => x._2 == max).collect()(0)._1
    val minimum = elements.filter(x => x._2 == min).collect()(0)._1
    (maximum, minimum)
  }
}

