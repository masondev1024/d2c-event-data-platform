"""The local-only runtime must reject remote deployment before starting it."""
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from lakehouse.d2c_iceberg_stream import create_spark_session, settings_from_environment


class LocalMasterTests(unittest.TestCase):
    def fake_pyspark(self, master):
        builder = MagicMock()
        for name in ("appName", "master", "config"):
            getattr(builder, name).return_value = builder
        spark = ModuleType("pyspark")
        conf = SimpleNamespace(get=lambda _key, default: default if master is None else master)
        spark.SparkConf = lambda: conf
        sql = ModuleType("pyspark.sql")
        sql.SparkSession = SimpleNamespace(builder=builder)
        return {"pyspark": spark, "pyspark.sql": sql}, builder

    def test_remote_master_rejected_before_context_creation(self):
        for master in ("yarn", "spark://remote:7077", "k8s://remote", "local-cluster[2,1,1024]", "local[0]"):
            modules, builder = self.fake_pyspark(master)
            with self.subTest(master=master), patch.dict(sys.modules, modules):
                with self.assertRaisesRegex(ValueError, "local Spark master"):
                    create_spark_session(settings_from_environment({}))
                builder.getOrCreate.assert_not_called()

    def test_local_modes_preserved(self):
        for master in ("local", "local[1]", "local[2]", "local[*]", "local[2,2]", None):
            modules, builder = self.fake_pyspark(master)
            with self.subTest(master=master), patch.dict(sys.modules, modules):
                result = create_spark_session(settings_from_environment({}))
                self.assertIs(result, builder.getOrCreate.return_value)
                builder.master.assert_called_once_with(master or "local[*]")
                builder.config.assert_any_call("spark.ui.enabled", "false")


if __name__ == "__main__": unittest.main()
