from copy import deepcopy
import os
import pytest
import sys
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
    # Each dict in the list will be the full JSON response from reading
    # the dx file object
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



if __name__=="__main__":
    TestFilterHighestConfigVersion()