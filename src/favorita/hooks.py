import logging

from delta import configure_spark_with_delta_pip
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder.
        """
        logger = logging.getLogger(__name__)
        # Load the spark configuration in spark.yaml using the config loader
        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_builder = SparkSession.builder.appName(context.project_path.name)
        configure_spark_with_delta_pip(spark_builder)
        spark_session_conf = spark_builder.enableHiveSupport().config(
            conf=spark_conf
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

        spark_ui_message = f"SparkUI: http://localhost:{parameters['spark.ui.port']}"

        logger.info(spark_ui_message)
