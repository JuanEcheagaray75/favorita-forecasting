import logging

from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkHooks:
    @hook_impl
    def after_context_created(self, context: KedroContext) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """
        logger = logging.getLogger(__name__)
        # Load the spark configuration in spark.yaml using the config loader
        parameters: dict[str, str] = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_builder = SparkSession.builder.appName(context.project_path.name)
        spark_session_conf = spark_builder.enableHiveSupport().config(conf=spark_conf)
        _spark_session: SparkSession = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

        logger.info(_spark_session.sparkContext.uiWebUrl)
