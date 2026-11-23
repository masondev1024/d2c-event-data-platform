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

    def test_explicit_bounded_choice_precedes_context_creation_when_launcher_defaults_are_not_visible(self):
        # Before the JVM exists, SparkConf has an empty Python dictionary even
        # though spark-submit already selected local[1] for its future JVM.
        modules, builder = self.fake_pyspark(None)
        launcher = {"spark.master": "local[1]"}
        choices, events = {}, []
        def choose(master):
            choices["spark.master"] = master
            events.append(("builder.master", master))
            return builder
        def initialize():
            effective = {**launcher, **choices}["spark.master"]
            events.append(("getOrCreate", effective))
            return SimpleNamespace(sparkContext=SimpleNamespace(master=effective))
        builder.master.side_effect = choose
        builder.getOrCreate.side_effect = initialize
        with patch.dict(sys.modules, modules):
            result = create_spark_session(settings_from_environment({}), local_master="local[1]")
        self.assertEqual(result.sparkContext.master, "local[1]")
        self.assertEqual(events, [("builder.master", "local[1]"), ("getOrCreate", "local[1]")])

    def test_explicit_invalid_or_conflicting_master_never_creates_a_context(self):
        for configured, selected in ((None, "yarn"), (None, "spark://remote:7077"), (None, "k8s://remote"),
                                     (None, "local-cluster[2,1,1024]"), (None, "local[0]"), (None, ""), (None, True),
                                     ("local[*]", "local[1]"), ("local[2]", "local[1]"), ("yarn", "local[1]")):
            modules, builder = self.fake_pyspark(configured)
            with self.subTest(configured=configured, selected=selected), patch.dict(sys.modules, modules):
                with self.assertRaises(ValueError):
                    create_spark_session(settings_from_environment({}), local_master=selected)
                builder.master.assert_not_called()
                builder.getOrCreate.assert_not_called()

    def test_matching_launcher_and_explicit_choice_are_preserved(self):
        modules, builder = self.fake_pyspark("local[1]")
        with patch.dict(sys.modules, modules):
            create_spark_session(settings_from_environment({}), local_master="local[1]")
        builder.master.assert_called_once_with("local[1]")

    def test_explicit_master_is_keyword_only(self):
        modules, builder = self.fake_pyspark(None)
        with patch.dict(sys.modules, modules), self.assertRaises(TypeError):
            create_spark_session(settings_from_environment({}), "local[1]")
        builder.getOrCreate.assert_not_called()

    def test_extra_configuration_cannot_override_or_bypass_the_local_choice(self):
        for master in ("yarn", "spark://remote:7077", "local[2]", "local[*]"):
            modules, builder = self.fake_pyspark(None)
            with self.subTest(master=master), patch.dict(sys.modules, modules):
                # The factory deliberately has no arbitrary extra_conf API.
                with self.assertRaises(TypeError):
                    create_spark_session(settings_from_environment({}), local_master="local[1]",
                                         extra_conf={"spark.master": master})
                builder.master.assert_not_called()
                builder.getOrCreate.assert_not_called()


if __name__ == "__main__": unittest.main()
