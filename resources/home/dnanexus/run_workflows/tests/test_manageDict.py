import json
import os
import sys
from tabnanny import check

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from run_workflows import manageDict
from tests import TEST_DATA_DIR

class TestFindAndReplaceInputs():
    """
    Tests for methods to search config for INPUT- and replace
    """
    def __init__(self) -> None:
        with open(os.path.join(TEST_DATA_DIR, 'test_low_level_config.json')) as fh:
            self.full_config = json.load(fh)


    def test_find_job_inputs(self) -> None:
        """
        Test recursive function that searches nested dict for inputs
        """
        t = manageDict().find_job_inputs(
            identifier='name', input_dict=self.full_config, check_key=True
        )
        print('done')
        print(f'\n\n\n\n')
        print(list(t))



if __name__ == '__main__':

    inputs = TestFindAndReplaceInputs()

    print(inputs.full_config)

    inputs.test_find_job_inputs()
