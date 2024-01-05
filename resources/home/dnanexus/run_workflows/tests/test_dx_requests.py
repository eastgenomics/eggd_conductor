from copy import deepcopy
import os
import unittest
from unittest.mock import patch
import sys

import pytest

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils.dx_requests import DXManage


class TestFilterHighestConfigVersion():
    """
    Tests for DXManage.filter_highest_config_version for that filters
    all JSON config files found for the highest version of each assay code
    """
    # Minimal test data structure of list of config files returned
    # from DXManage.get_json_configs(), including only assay, assay_code,
    # version and file_id that are required for filtering them down.
    # Each dict in the list would normally be the full JSON response
    # from reading the dx file object
    all_config_files = [
        {
            'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.0.0',
            'file_id': 'file-xxx'
        },
        {
            'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.1.0',
            'file_id': 'file-xxx'
        },
        {
            'assay': 'MYE', 'assay_code': 'EGG2|LAB123', 'version': '1.2.0',
            'file_id': 'file-xxx'
        },
        {
            'assay': 'MYE', 'assay_code': 'LAB123|LAB456', 'version': '1.3.0',
            'file_id': 'file-xxx'
        },
        {
            'assay': 'TSO500', 'assay_code': 'EGG5', 'version': '1.0.0',
            'file_id': 'file-xxx'
        },
        {
            'assay': 'TSO500', 'assay_code': 'EGG5', 'version': '1.1.0',
            'file_id': 'file-xxx'
        }
    ]


    def test_correct_filtered_configs(self):
        """
        Test that the above set of config files returns the expected configs
        for each assay code.

        Unique assay codes present between all configs:
            EGG2, EGG5, LAB123, LAB456

        We expect to return the following:
            - EGG2|LAB123 -> 1.2.0
            - LAB123|LAB456 -> 1.3.0
            - EGG5 -> 1.1.0

        Rationale of each single code matching:
            - subset of unique individual codes: EGG2, EGG5, LAB123 & LAB456
            - matches for each:
                - EGG2      ->  matches in EGG2|LAB123 (1.2.0)
                - EGG5      ->  matches in EGG5 (1.1.0)
                - LAB123    ->  matches in LAB123|LAB456 (1.3.0)
                - LAB456    ->  matches in LAB123|LAB456 (1.3.0)
        """
        correct_configs = {
            'EGG2|LAB123': {
                'assay': 'MYE', 'assay_code': 'EGG2|LAB123', 'version': '1.2.0',
                'file_id': 'file-xxx'
            },
            'LAB123|LAB456': {
                'assay': 'MYE', 'assay_code': 'LAB123|LAB456', 'version': '1.3.0',
                'file_id': 'file-xxx'
            },
            'EGG5': {
                'assay': 'TSO500', 'assay_code': 'EGG5', 'version': '1.1.0',
                'file_id': 'file-xxx'
            }
        }

        filtered_configs = DXManage.filter_highest_config_version(
            self.all_config_files)

        assert filtered_configs == correct_configs, (
            "Incorrect configs filtered"
        )


    def test_correct_versions_kept_same_assay_code(self):
        """
        Test that correct versions of same config file kept when comparing
        which is higher when config files have same assay code
        """
        configs = [
            {
                'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.0.0',
                'file_id': 'file-xxx'
            },
            {
                'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.1.0',
                'file_id': 'file-xxx'
            },
            {
                'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.11.0',
                'file_id': 'file-xxx'
            }
        ]

        correct_output = {
            'EGG2': {
                'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.11.0',
                'file_id': 'file-xxx'
            }
        }

        filtered = DXManage.filter_highest_config_version(configs)

        assert filtered == correct_output, (
            'Wrong version of config file returned'
        )


    def test_correct_versions_kept_different_assay_code(self):
        """
        Test that correct version of config file returned when the assay codes
        differ but one is a subset of another with a lower version (should
        only return the higher version config)
        """
        configs = [
            {
                'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.0.0',
                'file_id': 'file-xxx'
            },
            {
                'assay': 'MYE', 'assay_code': 'EGG2|LAB123', 'version': '1.11.0',
                'file_id': 'file-xxx'
            }
        ]

        correct_output = {
            'EGG2|LAB123': {
                'assay': 'MYE', 'assay_code': 'EGG2|LAB123', 'version': '1.11.0',
                'file_id': 'file-xxx'
            }
        }

        filtered = DXManage.filter_highest_config_version(configs)

        assert filtered == correct_output, (
            'Wrong version of config file returned for different assay codes'
        )


    def test_assert_raised_with_two_configs_of_same_version(self):
        """
        Tests when 2 config files with the sam version are found for a
        given assay code an AssertionError is raised.

        i.e. EGG2 in the following {'EGG2': 1.2.0, 'EGG2|LAB123': 1.2.0}
        """
        # add in a conflicting config file to the returned list
        config_files = deepcopy(self.all_config_files)
        config_files.append({
            'assay': 'MYE', 'assay_code': 'EGG2', 'version': '1.2.0',
            'file_id': 'file-xxx'})

        with pytest.raises(AssertionError):
            DXManage.filter_highest_config_version(config_files)


    def test_assert_raised_on_missing_assay_code(self):
        """
        Tests when assay_code key is missing from a config that an
        AssertionError is raised
        """
        config_files = deepcopy(self.all_config_files)
        config_files.append({
            'assay': 'test', 'version': '1.0.0', 'file_id': 'file-xxx'})

        with pytest.raises(AssertionError):
            DXManage.filter_highest_config_version(config_files)


    def test_assert_raised_on_missing_version(self):
        """
        Tests when version key is missing from a config that an
        AssertionError is raised
        """
        config_files = deepcopy(self.all_config_files)
        config_files.append({
            'assay': 'test', 'assay_code': 'TEST', 'file_id': 'file-xxx'})

        with pytest.raises(AssertionError):
            DXManage.filter_highest_config_version(config_files)


class TestDXManageGetJobOutputDetails(unittest.TestCase):
    """
    Tests for DXManage.get_job_output_details()

    Function queries the output directory of a given job for output
    files, then filters these down by the given job ID to ensure
    they are output from that job. This is to get all the output
    file details in an efficient manner since dxpy.describe on a
    job can't return the output file details directly

    Tests will just be to show that only files for the given job
    are correctly returned
    """
    @patch('utils.dx_requests.dx.DXJob')
    @patch('utils.dx_requests.dx.find_data_objects')
    def test_only_job_specified_job_files_returned(self, mock_find, mock_job):
        """
        Test when a directory has other job output in that only the
        output from the given job is returned
        """
        # minimal return of dx.describe on given job ID
        mock_job.return_value.describe.return_value = {
            'id': 'job-xxx',
            'project': 'project-xxx',
            'output': [
                {
                    'output_field1': [
                        {'$dnanexus_link': 'file-xxx'}
                    ]
                }
            ]
        }

        # minimal dx.find_data_objects return with files from multiple jobs
        mock_find.return_value = [
            {
                'id': 'file-xxx',
                'describe': {
                    'createdBy': {
                        'job': 'job-xxx'
                    }
                }
            },
            {
                'id': 'file-yyy',
                'describe': {
                    'createdBy': {
                        'job': 'job-yyy'
                    }
                }
            },
            {
                'id': 'file-zzz',
                'describe': {
                    'createdBy': {
                        'job': 'job-zzz'
                    }
                }
            }
        ]

        files, ids = DXManage(None).get_job_output_details('job-xxx')

        with self.subTest():
            self.assertEqual(files, [mock_find.return_value[0]])

        with self.subTest():
            correct_ids = [
                {
                    'output_field1': [
                        {'$dnanexus_link': 'file-xxx'}
                    ]
                }
            ]
            self.assertEqual(ids, correct_ids)



if __name__=="__main__":
    TestFilterHighestConfigVersion()
