import json
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils.manage_dict import PPRINT, ManageDict
from tests import TEST_DATA_DIR


class TestSearchDict():
    """
    Tests for ManageDict.search() that searches a given dictionary for a
    given pattern against either keys or values, and can return the
    keys or values
    """
    with open(os.path.join(TEST_DATA_DIR, 'test_low_level_config.json')) as fh:
        full_config = json.load(fh)

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
        of dicts and return each value
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
    with open(os.path.join(TEST_DATA_DIR, 'test_low_level_config.json')) as fh:
        full_config = json.load(fh)

    def test_replace_level1_keys(self):
        """
        Test replacing the first level of keys in the dictionary
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

def test_filter_job_outputs_dict():
    """
    Test filtering job outputs -> inputs dict by given pattern(s)
    """
    # dict of per sample jobs launched as built in app
    job_outputs_dict = {
        '2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2': {
            'analysis_1': 'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2'
        },
        'Oncospan-158-1-AA1-BBB-MYE-U-EGG2': {
            'analysis_1': 'analysis-GGjgz004Bv4P8yqJGp9pyyqb'},
            'analysis_2': 'job-GGjgz1j4Bv48yF89GpZ6zkGz'
        }

    # dict matching section as would be in config defining the stage
    # input and patterns to filter by
    filter_dict = {
        "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": [
            "Oncospan*"
        ],
        "another_stage": [
            "2207155-22207Z0091*"
        ]
    }

    filtered_output = ManageDict().filter_job_outputs_dict(
        stage='stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file',
        outputs_dict=job_outputs_dict,
        filter_dict=filter_dict
    )

    correct_output = {
       'Oncospan-158-1-AA1-BBB-MYE-U-EGG2': {
           'analysis_1': 'analysis-GGjgz004Bv4P8yqJGp9pyyqb'
        }
    }

    assert filtered_output == correct_output, (
        "Filtering outputs dict with filter_job_outputs_dict() incorrect"
    )
    def test_replace_all_value1(self):
        """
        Test searching replacing all values containing 'value1'
        """
        output = ManageDict().replace(
            input_dict=self.full_config,
            to_replace='value1',
            replacement='test',
            search_key=False,
            replace_key=False
        )

        correct_output = [
            {
                'A_level2': {
                    'A_level3_key1': 'test',
                    'A_level3_key2': [
                        'test',
                        'A_level3_array_value2',
                        'A_level3_array_value3',
                        'A_level3_array_value4'
                    ]
                }
            }, {
                'B_level2': {
                'B_level3_key1': 'test',
                'B_level3_key2': 'B_level3_value2',
                'B_level3_key3': 'B_level3_value3'
            }}, {'C_level2': [
                {'C_array1': 'test'},
                {'C_array1': 'C_array1_value2'},
                {'C_array1': 'C_array1_value3'}
            ]}
        ]

        assert list(output.values()) == correct_output, (
            "Searching and replacing 'value1' output incorrect"
        )

    def test_replace_value_from_key(self):
        """
        Test replacing all values from keys matching 'B_level3'
        """
        output = ManageDict().replace(
            input_dict=self.full_config,
            to_replace='B_level3',
            replacement='test',
            search_key=True,
            replace_key=False
        )

        correct_output = {
            'A_level1': {
                'A_level2': {
                    'A_level3_key1': 'A_level3_value1',
                    'A_level3_key2': [
                        'A_level3_array_value1',
                        'A_level3_array_value2',
                        'A_level3_array_value3',
                        'A_level3_array_value4'
                    ]
                }
            },
            'B_level1': {
                'B_level2': {
                    'B_level3_key1': 'test',
                    'B_level3_key2': 'test',
                    'B_level3_key3': 'test'}},
            'C_level1': {
                'C_level2': [
                    {'C_array1': 'C_array1_value1'},
                    {'C_array1': 'C_array1_value2'},
                    {'C_array1': 'C_array1_value3'}
                ]
            }
        }

        assert output == correct_output, (
            'Searching keys and replacing values returned wrong output'
        )



if __name__ == '__main__':

    inputs = TestSearchDict()

    inputs.test_search_key_return_key_level1()
    inputs.test_search_key_return_value_level1()
    inputs.test_search_key_return_array_values()
    inputs.test_search_dict_array()

    replace = TestReplaceDict()
    replace.test_replace_level1_keys()

    test_filter_job_outputs_dict()
    replace.test_replace_value_from_key()
