package com.github.vitalibo.glue

import org.slf4j.{Logger, LoggerFactory}

object Driver {
  private val logger: Logger = LoggerFactory.getLogger(Driver.getClass)
  private val factory: Factory = Factory.getInstance()

  def main(args: Array[String]): Unit = {
    try {
      val spark = factory.createSpark(args)

      spark.submit(
        factory.createJob(args))

    } catch {
      case e: Exception =>
        logger.error("Job failed execution", e)
        throw e
    }
  }
}
