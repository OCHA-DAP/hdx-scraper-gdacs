import filecmp
from datetime import datetime, timezone
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.gdacs.gdacs import GDACS


class TestGDACS:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "gdacs", "config")

    def test_gdacs(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestGDACS",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                gdacs = GDACS(configuration, retriever)
                last_build_date, update = gdacs.parse_feed(
                    datetime(2024, 12, 1, 0, 0, tzinfo=timezone.utc)
                )
                assert last_build_date == datetime(
                    2024, 12, 10, 21, 15, 3, tzinfo=timezone.utc
                )
                assert update is True
                assert len(gdacs.data) == 2

                dataset = gdacs.generate_dataset()
                dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))
                assert dataset == {
                    "name": "gdacs-rss-information",
                    "title": "GDACS RSS information",
                    "dataset_date": "[2024-12-09T00:00:00 TO 2024-12-10T23:59:59]",
                    "tags": [
                        {
                            "name": "cyclones-hurricanes-typhoons",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "earthquake-tsunami",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "natural disasters",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "usa"}],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "https://gdacs.org/Knowledge/overview.aspx",
                    "caveats": "While we try everything to ensure accuracy, this "
                    "information is purely indicative and should not be used for any "
                    "decision making without alternate sources of information. The JRC "
                    "is not responsible for any damage or loss resulting from use of the "
                    "information presented on this website.",
                    "dataset_source": "European Union",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "f27b8618-52b9-4827-9440-eb65a1f66d41",
                    "data_update_frequency": 1,
                    "notes": "Disaster alerts in the past 4 days. European "
                    "Union, 2024",
                    "subnational": "1",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "gdacs_rss_information.csv",
                        "description": " ",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]

                assert filecmp.cmp(
                    join(fixtures_dir, "gdacs_rss_information.csv"),
                    join(tempdir, "gdacs_rss_information.csv"),
                )
