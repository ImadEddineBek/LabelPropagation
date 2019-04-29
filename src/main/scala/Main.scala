import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, GraphLoader, Pregel, VertexId}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.rdd.RDDFunctions
import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.log
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.LabelPropagation
import org.spark_project.dmg.pmml.True

import scala.reflect.ClassTag


object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val data_path = args(0)
    val spark = SparkSession
      .builder()
      .appName("Label Propagation")
      .config("spark.master", "local[2]")
      .getOrCreate()
    var gr = GraphLoader.edgeListFile(spark.sparkContext, data_path)
    val init_message = Map[Long, Long]()


    //    val g = Pregel(
    //      graph = gr.mapVertices { case (vid, _) => vid.toLong },
    //      initialMsg = init_message,
    //      maxIterations = 2
    //      ,
    //    )(
    //      vprog = (id: Long, vd: Long, a: Map[Long, Long]) => {
    //        //        if (a.isEmpty)
    //        //          print()
    //        //        else {
    //        //          print("vprof" + id + "   " + vd + "  => " + a.maxBy(_._2)._1 + "  ")
    //        //          a.foreach(x => print(id + " " + x + " "))
    //        //        }
    //        //        println()
    //        if (a.isEmpty) vd else a.maxBy(_._2)._1
    //      },
    //      sendMsg = (e: EdgeTriplet[Long, Int]) => {
    //
    //        Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    //      },
    //      mergeMsg = (a: Map[Long, Long], b: Map[Long, Long]) => {
    //
    //        (a.keySet ++ b.keySet).map { i =>
    //          val count1Val = a.getOrElse(i, 0L)
    //          val count2Val = b.getOrElse(i, 0L)
    //          i -> (count1Val + count2Val)
    //        }(collection.breakOut)
    //      }
    //    )
    //
    //    val stats = analyze(g)
    //    val max = stats._4
    //    val min = stats._3
    //    val mean = stats._1
    //    val variance = stats._2
    //    val values = arg_max_min(g,max,min)
    //    val minimum = values._2
    //    val maximum = values._1
    //    g.vertices.filter(x => x._2 == maximum).foreach(println)
    //    g.vertices.filter(x => x._2 == minimum).foreach(println)

    FancyLabelPropagation.run(gr, maxSteps = 52, partitions = 2).vertices.foreach(println)

  }

  def analyze(g: Graph[Long, Int]) = {
    val nb_elements = g.vertices.groupBy(x => x._2).map(x => x._2.count(x => x == x))
    (nb_elements.mean(), nb_elements.variance(), nb_elements.min(), nb_elements.max())
  }

  def arg_max_min(g: Graph[Long, Int], max: Int, min: Int) = {
    val elements = g.vertices.groupBy(x => x._2).map(x => (x._1, x._2.count(x => x == x)))
    val maximum = elements.filter(x => x._2 == max).collect()(0)._1
    val minimum = elements.filter(x => x._2 == min).collect()(0)._1
    (maximum, minimum)
  }
}


import com.google.common.hash.Hashing.murmur3_32

object FancyLabelPropagation {
  type Message = Map[VertexId, Long]

  case class GlobalState(
                          iteration: Int,
                          partition: Int
                        )

  type VertexLabel = Option[Long]

  def sustainDecayProbCurve(transition: Int, end: Int)(iteration: Int): Double = {
    //println(s"transition: $transition end: $end iteration: $iteration")
    if (iteration <= transition) {
      1.0
    } else if (transition < iteration && iteration < end) {
      val intermediate = 1.0 - ((iteration.toDouble - transition.toDouble) / (end.toDouble - transition.toDouble))
      //println(s"intermediate: $intermediate")
      intermediate
    } else {
      0.0
    }
  }

  def alwaysAccept(_iteration: Int): Double = {
    1.0
  }

  def deoptionize[ED](graph: Graph[VertexLabel, ED]): Graph[Long, ED] = {
    graph mapVertices {
      case (vid, vlbl) => vlbl.getOrElse(-1)
    }
  }

  /**
    * Run static Label Propagation for detecting communities in networks.
    *
    * Each node in the network is initially assigned to its own community. At every superstep, nodes
    * send their community affiliation to all neighbors and update their state to the mode community
    * affiliation of incoming messages.
    *
    * LPA is a standard community detection algorithm for graphs. It is very inexpensive
    * computationally, although (1) convergence is not guaranteed and (2) one can end up with
    * trivial solutions (all nodes are identified into a single community).
    *
    * @tparam ED the edge attribute type (not used in the computation)
    * @param graph      the graph for which to compute the community affiliation
    * @param partitions how many graph partitions to create
    * @param maxSteps   the number of supersteps of LPA to be performed. Because this is a static
    * @param seed       the seed to use for creating the graph partitions
    * @param reshuffle  whether to reshuffle the paritioning each iteration
    *                   implementation, the algorithm will run for exactly this many supersteps.
    * @return a graph with vertex attributes containing the label of community affiliation
    */
  def run[VD, ED: ClassTag](
                             graph: Graph[VD, ED], partitions: Int, maxSteps: Int,
                             seed: Int = 0, reshuffle: Boolean = false,
                             acceptanceProb: Int => Double = alwaysAccept): Graph[VertexLabel, ED] = {
    val lpaGraph = graph.mapVertices {
      case (vid, _) => Some(vid): Option[Long]
    }

    propagate(lpaGraph, partitions, maxSteps, seed, reshuffle, false, acceptanceProb)
  }

  /**
    * Run LPA seeded with a few authorities, assumed to be serving different
    * communities. Most parameters as for `run'.
    *
    * @param labeledVids a set containing the initial authorities
    **/
  def runAuthorities[VD, ED: ClassTag](
                                        graph: Graph[VD, ED], labeledVids: Set[VertexId], partitions: Int, maxSteps: Int,
                                        seed: Int = 0, reshuffle: Boolean = false,
                                        acceptanceProb: Int => Double = alwaysAccept): Graph[VertexLabel, ED] = {
    val lpaGraph = graph.mapVertices {
      case (vid, _) =>
        if (labeledVids contains vid) {
          Some(vid)
        } else {
          None
        }
    };

    val expandedGraph = propagate(lpaGraph, partitions, maxSteps, seed, reshuffle, false, acceptanceProb)
    propagate(expandedGraph, partitions, maxSteps, seed, reshuffle, true, acceptanceProb)
  }

  /**
    * Internal method serving as workhorse for the others. Most parameters as
    * for `run'.
    *
    * @param sweep when true, only propagate to unlabelled nodes
    **/
  def propagate[ED: ClassTag](
                               graph: Graph[VertexLabel, ED], partitions: Int, maxSteps: Int,
                               seed: Int = 0, reshuffle: Boolean = false, sweep: Boolean = false,
                               acceptanceProb: Int => Double = alwaysAccept): Graph[VertexLabel, ED] = {

    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val hashFunc = murmur3_32(seed)

    def sendMessage(state: GlobalState)(e: EdgeTriplet[VertexLabel, ED]): Iterator[(VertexId, Message)] = {
      def getMsg(srcAttr: VertexLabel, dstAttr: VertexLabel, dstId: VertexId) = {
        srcAttr.filter(_ => (!sweep && acceptanceProb(state.iteration) > 0) || dstAttr.nonEmpty).map({
          srcLabel => Iterator((dstId, Map(srcLabel -> 1L)))
        }).getOrElse(Iterator.empty)
      }

      /* TODO: Make use of acceptanceProb
      state.iteration
      var hasher = hashFunc.newHasher()
      hasher.putLong(vid)
      */
      val forwardMsg = getMsg(e.srcAttr, e.dstAttr, e.dstId);
      val backwardsMsg = getMsg(e.dstAttr, e.srcAttr, e.srcId);
      forwardMsg ++ backwardsMsg
    }

    def mergeMessage(count1: Message, count2: Message)
    : Message = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }(collection.breakOut) // more efficient alternative to [[collection.Traversable.toMap]]
    }

    def vertexProgram(state: GlobalState)(vid: VertexId, attr: VertexLabel, message: Message): VertexLabel = {
      var hasher = hashFunc.newHasher()
      hasher.putLong(vid)
      if (reshuffle) {
        hasher.putInt(state.iteration)
      }
      val h = hasher.hash().asInt() & 0x00000000ffffffffL
      //println(h);
      val lower = (state.partition.toLong << 32L) / partitions.toLong
      //println(lower);
      val upper = ((state.partition + 1).toLong << 32L) / partitions.toLong
      //println(upper);
      val active = lower <= h && h < upper

      def receive(): VertexLabel = {
        Some(message.maxBy {
          case (label, count) => {
            count
          }
        }._1)
      }

      if (!message.isEmpty && active) {
        val accept = acceptanceProb(state.iteration)
        if (accept >= 1.0 || attr.isEmpty) {
          receive()
        } else {
          var hasher = hashFunc.newHasher()
          hasher.putLong(vid)
          hasher.putInt(state.iteration)
          // Uncorrelate
          hasher.putInt(0x11bf6e28)
          val h = hasher.hash().asInt() & 0x00000000ffffffffL
          if (h < accept * 0x0000000100000000L) {
            receive()
          } else {
            attr
          }
        }
      } else {
        attr
      }
    }

    def nextState(state: GlobalState): GlobalState = {
      val nextPartition = state.partition + 1
      if (nextPartition >= partitions)
        GlobalState(
          state.iteration + 1,
          0)
      else
        GlobalState(
          state.iteration,
          nextPartition)

    }

    val initialMessage = Map[VertexId, Long]()
    val initialState = GlobalState(0, 0)
    FancyPregel(graph, initialMessage, initialState, maxIterations = maxSteps * partitions)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage,
      nextState = nextState)
  }
}


/*
 * This file has been modified from Spark GraphX to have some global state
 * which is broadcast each iteration.
 *
 * Modifications by Frankie Robertson 2018. The notice below applies to the
 * original code.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */


import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
  * Implements a Pregel-like bulk-synchronous message-passing API.
  *
  * Unlike the original Pregel API, the GraphX Pregel API factors the sendMessage computation over
  * edges, enables the message sending computation to read both vertex attributes, and constrains
  * messages to the graph structure.  These changes allow for substantially more efficient
  * distributed execution while also exposing greater flexibility for graph-based computation.
  *
  * @example We can use the Pregel abstraction to implement PageRank:
  *          {{{
  * val pagerankGraph: Graph[Double, Double] = graph
  *   // Associate the degree with each vertex
  *   .outerJoinVertices(graph.outDegrees) {
  *     (vid, vdata, deg) => deg.getOrElse(0)
  *   }
  *   // Set the weight on the edges based on the degree
  *   .mapTriplets(e => 1.0 / e.srcAttr)
  *   // Set the vertex attributes to the initial pagerank values
  *   .mapVertices((id, attr) => 1.0)
  *
  * def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
  *   resetProb + (1.0 - resetProb) * msgSum
  * def sendMessage(id: VertexId, edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
  *   Iterator((edge.dstId, edge.srcAttr * edge.attr))
  * def messageCombiner(a: Double, b: Double): Double = a + b
  * val initialMessage = 0.0
  * // Execute Pregel for a fixed number of iterations.
  * Pregel(pagerankGraph, initialMessage, numIter)(
  *   vertexProgram, sendMessage, messageCombiner)
  * }}}
  *
  */
object FancyPregel extends Logging {

  /**
    * Execute a Pregel-like iterative vertex-parallel abstraction.  The
    * user-defined vertex-program `vprog` is executed in parallel on
    * each vertex receiving any inbound messages and computing a new
    * value for the vertex.  The `sendMsg` function is then invoked on
    * all out-edges and is used to compute an optional message to the
    * destination vertex. The `mergeMsg` function is a commutative
    * associative function used to combine messages destined to the
    * same vertex.
    *
    * On the first iteration all vertices receive the `initialMsg` and
    * on subsequent iterations if a vertex does not receive a message
    * then the vertex-program is not invoked.
    *
    * This function iterates until there are no remaining messages, or
    * for `maxIterations` iterations.
    *
    * @tparam VD the vertex data type
    * @tparam ED the edge data type
    * @tparam A  the Pregel message type
    * @param graph           the input graph.
    * @param initialMsg      the message each vertex will receive at the first
    *                        iteration
    * @param maxIterations   the maximum number of iterations to run for
    * @param activeDirection the direction of edges incident to a vertex that received a message in
    *                        the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
    *                        out-edges of vertices that received a message in the previous round will run. The default is
    *                        `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
    *                        in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
    *                        *both* vertices received a message.
    * @param vprog           the user-defined vertex program which runs on each
    *                        vertex and receives the inbound message and computes a new vertex
    * value.  On the first iteration the vertex program is invoked on
    *                        all vertices and is passed the default message.  On subsequent
    *                        iterations the vertex program is only invoked on those vertices
    *                        that receive messages.
    * @param sendMsg         a user supplied function that is applied to out
    *                        edges of vertices that received messages in the current
    *                        iteration
    * @param mergeMsg        a user supplied function that takes two incoming
    *                        messages of type A and merges them into a single message of type
    * A.  ''This function must be commutative and associative and
    *                        ideally the size of A should not increase.''
    * @return the resulting graph at the end of the computation
    *
    */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag, G: ClassTag]
  (graph: Graph[VD, ED],
   initialMsg: A,
   initialState: G,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: G => (VertexId, VD, A) => VD,
   sendMsg: G => EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A,
   nextState: G => G)
  : Graph[VD, ED] = {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    val checkpointInterval = graph.vertices.sparkContext.getConf
      .getInt("spark.graphx.pregel.checkpointInterval", -1)
    var state = initialState
    var g = graph.mapVertices((vid, vdata) => vprog(state)(vid, vdata, initialMsg))
    // XXX: The checkpointers are private so had to comment this out
    //val graphCheckpointer = new PeriodicGraphCheckpointer[VD, ED](
    //checkpointInterval, graph.vertices.sparkContext)
    //graphCheckpointer.update(g)

    // compute the messages
    var messages = mapReduceTriplets(g, sendMsg(state), mergeMsg)
    //val messageCheckpointer = new PeriodicRDDCheckpointer[(VertexId, A)](
    //checkpointInterval, graph.vertices.sparkContext)
    //messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])
    var activeMessages = messages.count()

    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      state = nextState(state)
      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog(state))
      //graphCheckpointer.update(g)

      val oldMessages = messages
      // Send new messages, skipping edges where neither side received a message. We must cache
      // messages so it can be materialized on the next line, allowing us to uncache the previous
      // iteration.
      messages = mapReduceTriplets(
        g, sendMsg(state), mergeMsg)
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
      //messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])
      activeMessages = messages.count()

      logInfo("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
    }
    //messageCheckpointer.unpersistDataSet()
    //graphCheckpointer.deleteAllCheckpoints()
    //messageCheckpointer.deleteAllCheckpoints()
    g
  } // end of apply

  def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
                                                                  g: Graph[VD, ED],
                                                                  mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                                  reduceFunc: (A, A) => A): VertexRDD[A] = {
    def sendMsg(ctx: EdgeContext[VD, ED, A]) {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }
    // XXX: Changed from g.aggregateMessagesWithActiveSet since it's private --
    // so probably less efficient
    // See https://issues.apache.org/jira/browse/SPARK-15739
    g.aggregateMessages(sendMsg, reduceFunc, TripletFields.All)
  }

}