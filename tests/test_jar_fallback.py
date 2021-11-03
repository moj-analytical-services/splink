import pytest
from splink.jar_fallback import jw_sim_py, jc_sim_py


def test_fallback_jw_nodata():
    assert jw_sim_py(None, None) == 0.0
    assert jw_sim_py("something", None) == 0.0
    assert jw_sim_py(None, "Something") == 0.0


def test_fallback_jc_nodata():
    assert jc_sim_py(None, None) == 0.0
    assert jc_sim_py("something", None) == 0.0
    assert jc_sim_py(None, "Something") == 0.0


def test_fallback_jw_wikipedia_examples():
    assert jw_sim_py("fly", "ant") == 0.0
    assert jw_sim_py("elephant", "hippo") == 0.44
    assert jw_sim_py("ABC Corporation", "ABC Corp") == 0.91
    assert jw_sim_py("PENNSYLVANIA", "PENNCISYLVNIA") == 0.9
    assert jw_sim_py("D N H Enterprises Inc", "D & H Enterprises, Inc.") == 0.93
    assert (
        jw_sim_py("My Gym Children's Fitness Center", "My Gym. Childrens Fitness")
        == 0.94
    )
