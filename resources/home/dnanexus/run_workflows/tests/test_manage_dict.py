import json
import os
import sys

import flatten_json

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils.manage_dict import ManageDict
from tests import TEST_DATA_DIR


class TestSearchDict():
    """
    Tests for ManageDict.search() that searches a given dictionary for a
    given pattern against either keys or values, and can return the
    keys or values
    """
    def __init__(self) -> None:
        with open(os.path.join(TEST_DATA_DIR, 'test_low_level_config.json')) as fh:
            self.full_config = json.load(fh)

    def test_search_key_return_key_level1(self) -> None:
        """
        Test ManageDict().search() against first level, checking
        and returning keys
        """
        output = ManageDict().search(
            identifier='level1',
            input_dict=self.full_config,
            check_key=True,
            return_key=True
        )

        correct_output = ['A_level1', 'B_level1', 'C_level1']

        assert sorted(output) == correct_output, (
            'Wrong keys returned checking keys with identifier "level1"'
        )

    def test_search_key_return_value_level1(self) -> None:
        """
        Test ManageDict().search() against first level, checking
        keys and returning values
        """
        output = ManageDict().search(
            identifier='level1',
            input_dict=self.full_config,
            check_key=True,
            return_key=False
        )

        correct_output = [
            'A_level3_array_value1', 'A_level3_array_value2',
            'A_level3_array_value3', 'A_level3_array_value4',
            'A_level3_value1', 'B_level3_value1', 'B_level3_value2',
            'B_level3_value3', 'C_array1_value1', 'C_array1_value2',
            'C_array1_value3'
        ]

        assert sorted(output) == correct_output, (
            'Wrong values returned checking keys with identifier "level1"'
        )

    def test_search_key_return_array_values(self) -> None:
        """
        Test ManageDict().search() against for key where values are an array
        """
        output = ManageDict().search(
            identifier='A_level3_key2',
            input_dict=self.full_config,
            check_key=True,
            return_key=False
        )

        correct_output = [
            'A_level3_array_value1', 'A_level3_array_value2',
            'A_level3_array_value3', 'A_level3_array_value4'
        ]

        assert sorted(output) == correct_output, (
            'Wrong values returned checking array of values'
        )

    def test_search_dict_array(self) -> None:
        """
        Test ManageDict().search() against for key where values are an array
        of dicts andfreturn each value
        """
        output = ManageDict().search(
            identifier='C_array1',
            input_dict=self.full_config,
            check_key=True,
            return_key=False
        )

        correct_output = [
            'C_array1_value1', 'C_array1_value2', 'C_array1_value3'
        ]

        assert sorted(output) == correct_output, (
            'Wrong values returned checking array of dict values'
        )

class TestReplaceDict():
    """
    Tests for ManageDict.replace() that searches a dictionaries keys or values
    for a pattern, and replaces it with another given pattern
    """
    def __init__(self) -> None:
        with open(os.path.join(TEST_DATA_DIR, 'test_low_level_config.json')) as fh:
            self.full_config = json.load(fh)

    def test_replace_level1_keys(self):
        """
        _summary_
        """
        output = ManageDict().replace(
            input_dict=self.full_config,
            to_replace='level1',
            replacement='test',
            search_key=True,
            replace_key=True
        )

        # replacing all level1 keys with same so should only be one key
        assert list(output.keys()) == ['test'], (
            "Replacing level1 keys not correct"
        )



if __name__ == '__main__':

    inputs = TestSearchDict()

    inputs.test_search_key_return_key_level1()
    inputs.test_search_key_return_value_level1()
    inputs.test_search_key_return_array_values()
    inputs.test_search_dict_array()

    replace = TestReplaceDict()
    replace.test_replace_level1_keys()
