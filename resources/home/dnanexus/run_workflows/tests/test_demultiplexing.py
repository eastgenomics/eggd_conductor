from unittest.mock import MagicMock, patch

import pytest
from utils.demultiplexing import (
    get_demultiplex_job_details,
    instance_type_tiebreaker,
    set_config_for_demultiplexing,
)


class TestSetConfigForDemultiplexing:
    def test_no_configs(self):
        output = set_config_for_demultiplexing({"not_demultiplex_config": 1})

        assert output is None

    def test_no_instance_type(self):
        output = set_config_for_demultiplexing(
            {"demultiplex_config": {"not_instance_type": 1}}
        )

        assert output == None

    def test_additional_args_no_instance_type(self):
        output = set_config_for_demultiplexing(
            {
                "assay": "CEN",
                "demultiplex_config": {"additional_args": "--foo"},
            },
            {"assay": "TWE", "demultiplex_config": {}},
        )

        assert output == {"additional_args": "--foo"}

    def test_same_instance_type_additional_args(self):
        output = set_config_for_demultiplexing(
            {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x16"}},
            {
                "demultiplex_config": {
                    "instance_type": "mem1_ssd1_v2_x16",
                    "additional_args": "--foo",
                }
            },
        )

        assert output == {
            "instance_type": "mem1_ssd1_v2_x16",
            "additional_args": "--foo",
        }

    def test_higher_instance_type(self):
        output = set_config_for_demultiplexing(
            {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x16"}},
            {"demultiplex_config": {"instance_type": "mem2_ssd1_v2_x16"}},
        )

        assert output == {"instance_type": "mem2_ssd1_v2_x16"}

    def test_biggest_instance_type_wins_over_args(self):
        output = set_config_for_demultiplexing(
            {
                "demultiplex_config": {
                    "instance_type": "mem1_ssd1_v2_x16",
                    "additional_args": "--foo",
                }
            },
            {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x72"}},
        )

        assert output == {"instance_type": "mem1_ssd1_v2_x72"}

    def test_single_instance_type(self):
        output = set_config_for_demultiplexing(
            {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x16"}}
        )

        assert output == {"instance_type": "mem1_ssd1_v2_x16"}

    def test_select_highest_instance_type_config(self):
        output = set_config_for_demultiplexing(
            {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x16"}},
            {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x72"}},
            {"demultiplex_config": {"instance_type": "mem1_ssd2_v2_x36"}},
        )

        assert output == {"instance_type": "mem1_ssd1_v2_x72"}

    def test_app_id_as_tiebreaker(self):
        output = set_config_for_demultiplexing(
            {
                "demultiplex_config": {
                    "instance_type": "mem1_ssd1_v2_x16",
                    "additional_args": "--foo",
                    "app_id": "app-id",
                }
            },
            {
                "demultiplex_config": {
                    "instance_type": "mem1_ssd1_v2_x16",
                    "additional_args": "--foo",
                }
            },
        )

        assert output == {
            "instance_type": "mem1_ssd1_v2_x16",
            "additional_args": "--foo",
            "app_id": "app-id",
        }

    def test_additional_args_exception(self):
        with pytest.raises(
            AssertionError,
            match=("Multiple conflicting additional args specified"),
        ):
            set_config_for_demultiplexing(
                {
                    "demultiplex_config": {
                        "instance_type": "mem1_ssd1_v2_x16",
                        "additional_args": "--foo",
                    }
                },
                {
                    "demultiplex_config": {
                        "instance_type": "mem1_ssd1_v2_x16",
                        "additional_args": "--bar",
                    }
                },
            )

    def test_app_id_exception(self):
        with pytest.raises(
            AssertionError,
            match=("Multiple conflicting app ids specified"),
        ):
            set_config_for_demultiplexing(
                {
                    "demultiplex_config": {
                        "instance_type": "mem1_ssd1_v2_x16",
                        "additional_args": "--foo",
                        "app_id": "app-id1",
                    }
                },
                {
                    "demultiplex_config": {
                        "instance_type": "mem1_ssd1_v2_x16",
                        "additional_args": "--foo",
                        "app_id": "app-id2",
                    }
                },
            )

    def test_identical_configs_no_app_id(self):
        demultiplex_config = {
            "instance_type": "mem2_ssd1_v2_x48",
            "additional_args": "--foo",
        }

        output = set_config_for_demultiplexing(
            {"demultiplex_config": dict(demultiplex_config)},
            {"demultiplex_config": dict(demultiplex_config)},
        )

        assert output == demultiplex_config

    def test_identical_configs_with_app_id(self):
        demultiplex_config = {
            "instance_type": "mem2_ssd1_v2_x48",
            "additional_args": "--foo",
            "app_id": "app-id",
        }

        output = set_config_for_demultiplexing(
            {"demultiplex_config": dict(demultiplex_config)},
            {"demultiplex_config": dict(demultiplex_config)},
        )

        assert output == demultiplex_config

    def test_smaller_instance_type_conflicting_args_ignored(self):
        output = set_config_for_demultiplexing(
            {
                "demultiplex_config": {
                    "instance_type": "mem2_ssd1_v2_x48",
                    "additional_args": "--foo",
                }
            },
            {
                "demultiplex_config": {
                    "instance_type": "mem2_ssd1_v2_x48",
                    "additional_args": "--foo",
                }
            },
            {
                "demultiplex_config": {
                    "instance_type": "mem2_ssd1_v2_x32",
                    "additional_args": "--bar",
                }
            },
        )

        assert output == {
            "instance_type": "mem2_ssd1_v2_x48",
            "additional_args": "--foo",
        }

    def test_smaller_instance_type_conflicting_app_id_ignored(self):
        output = set_config_for_demultiplexing(
            {
                "demultiplex_config": {
                    "instance_type": "mem2_ssd1_v2_x48",
                    "app_id": "app-id1",
                }
            },
            {
                "demultiplex_config": {
                    "instance_type": "mem2_ssd1_v2_x48",
                    "app_id": "app-id1",
                }
            },
            {
                "demultiplex_config": {
                    "instance_type": "mem2_ssd1_v2_x32",
                    "app_id": "app-id2",
                }
            },
        )

        assert output == {
            "instance_type": "mem2_ssd1_v2_x48",
            "app_id": "app-id1",
        }

    def test_non_identical_configs_left_exception(self):
        with pytest.raises(
            Exception,
            match=("Couldn't select a demultiplex config"),
        ):
            set_config_for_demultiplexing(
                {
                    "demultiplex_config": {
                        "instance_type": "mem2_ssd1_v2_x48",
                        "app_name": "app-name1",
                    }
                },
                {
                    "demultiplex_config": {
                        "instance_type": "mem2_ssd1_v2_x48",
                        "app_name": "app-name2",
                    }
                },
            )


@patch("utils.demultiplexing.dx.search.find_data_objects")
@patch("utils.demultiplexing.dx.bindings.dxjob.DXJob")
def test_get_demultiplex_job_details(mock_job, mock_data_objects):
    mock_job.return_value = MagicMock(
        describe=MagicMock(project="project_name", folder="folder_name")
    )
    mock_data_objects.return_value = (
        {
            "id": "id1",
            "describe": {"name": "name1"},
        },
        {
            "id": "id2",
            "describe": {"name": "name2"},
        },
        {
            "id": "id3",
            "describe": {"name": "Undetermined_name2"},
        },
    )

    expected_output = [("id1", "name1"), ("id2", "name2")]

    output = get_demultiplex_job_details("")

    assert output == expected_output


class TestInstanceTypeTiebreaker:
    def test_no_instance_type(self):
        input = [{}, {}]
        output = instance_type_tiebreaker(*input)
        assert output == []

    def test_different_key_than_instance_type(self):
        input = [
            {"not_instance_type": "mem1_ssd1_v2_x2"},
            {"another_instance_type": "mem1_ssd1_v2_x2"},
        ]
        output = instance_type_tiebreaker(*input)
        assert output == []

    def test_one_instance_type(self):
        input = [{"instance_type": "mem1_ssd1_v2_x2"}, {}]
        output = instance_type_tiebreaker(*input)
        assert output == ["mem1_ssd1_v2_x2"]

    def test_identical_instance_type(self):
        input = [
            {"instance_type": "mem1_ssd1_v2_x2"},
            {"instance_type": "mem1_ssd1_v2_x2"},
        ]
        output = instance_type_tiebreaker(*input)
        assert output == ["mem1_ssd1_v2_x2", "mem1_ssd1_v2_x2"]

    def test_complex_tiebreaker(self):
        input = [
            {"instance_type": "mem1_ssd1_v2_x2"},
            {"instance_type": "mem3_ssd2_v2_x16"},
            {"instance_type": "mem3_ssd1_v1_x16"},
            {"instance_type": "mem3_ssd3_v2_x16"},
        ]
        output = instance_type_tiebreaker(*input)
        assert output == ["mem3_ssd3_v2_x16"]
