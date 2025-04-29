import pytest

from pipelines.components.common.component import *


def test_run_training_metadata_op():
    print(run_metadata_op.python_func(10))
