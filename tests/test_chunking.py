import pytest

from splink.internals.chunking import all_chunk_combinations, validate_chunk_param


class TestValidateChunkParam:
    def test_valid_tuples(self):
        assert validate_chunk_param((1, 3), "left_chunk") == (1, 3)
        assert validate_chunk_param((3, 3), "left_chunk") == (3, 3)
        assert validate_chunk_param((1, 1), "left_chunk") == (1, 1)
        assert validate_chunk_param((5, 10), "right_chunk") == (5, 10)

    def test_none_returns_none(self):
        assert validate_chunk_param(None, "left_chunk") is None

    def test_invalid_type_not_tuple(self):
        with pytest.raises(ValueError, match="must be a tuple"):
            validate_chunk_param([1, 3], "left_chunk")

        with pytest.raises(ValueError, match="must be a tuple"):
            validate_chunk_param(1, "left_chunk")

        with pytest.raises(ValueError, match="must be a tuple"):
            validate_chunk_param("(1, 3)", "left_chunk")

    def test_invalid_tuple_length(self):
        with pytest.raises(ValueError, match="must be a tuple"):
            validate_chunk_param((1,), "left_chunk")

        with pytest.raises(ValueError, match="must be a tuple"):
            validate_chunk_param((1, 2, 3), "left_chunk")

    def test_chunk_number_too_low(self):
        with pytest.raises(ValueError, match="chunk_number must be between 1 and 3"):
            validate_chunk_param((0, 3), "left_chunk")

    def test_chunk_number_too_high(self):
        with pytest.raises(ValueError, match="chunk_number must be between 1 and 3"):
            validate_chunk_param((4, 3), "left_chunk")

    def test_total_chunks_too_low(self):
        with pytest.raises(ValueError, match="total_chunks must be >= 1"):
            validate_chunk_param((1, 0), "left_chunk")

        with pytest.raises(ValueError, match="total_chunks must be >= 1"):
            validate_chunk_param((1, -1), "left_chunk")

    def test_non_integers(self):
        with pytest.raises(ValueError, match="must contain integers"):
            validate_chunk_param((1.5, 3), "left_chunk")

        with pytest.raises(ValueError, match="must contain integers"):
            validate_chunk_param((1, 3.0), "left_chunk")

        with pytest.raises(ValueError, match="must contain integers"):
            validate_chunk_param(("1", "3"), "left_chunk")


class TestAllChunkCombinations:
    def test_basic_2x3(self):
        result = list(all_chunk_combinations(2, 3))
        expected = [
            ((1, 2), (1, 3)),
            ((1, 2), (2, 3)),
            ((1, 2), (3, 3)),
            ((2, 2), (1, 3)),
            ((2, 2), (2, 3)),
            ((2, 2), (3, 3)),
        ]
        assert result == expected

    def test_single_chunks(self):
        result = list(all_chunk_combinations(1, 1))
        assert result == [((1, 1), (1, 1))]

    def test_correct_count(self):
        result = list(all_chunk_combinations(5, 7))
        assert len(result) == 5 * 7

    def test_is_iterator(self):
        gen = all_chunk_combinations(2, 2)
        # Should be an iterator, not a list
        assert hasattr(gen, "__iter__")
        assert hasattr(gen, "__next__")
