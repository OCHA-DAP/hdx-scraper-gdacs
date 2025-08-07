from datetime import datetime, timezone
from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.gdacs.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
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
                pipeline = Pipeline(configuration, retriever)
                last_build_date, update = pipeline.parse_feed(
                    datetime(2024, 12, 1, 0, 0, tzinfo=timezone.utc)
                )
                assert last_build_date == datetime(
                    2024, 12, 10, 21, 15, 3, tzinfo=timezone.utc
                )
                assert update is True
                assert len(pipeline.data) == 2

                dataset = pipeline.generate_dataset()
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "gdacs-rss-information",
                    "title": "GDACS RSS Information",
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
                    "notes": "GDACS alerts are issued for earthquakes and possible "
                    "subsequent tsunamis, tropical cyclones, floods and volcanoes. "
                    "Earthquake, tsunami and tropical cyclones calculations and "
                    "assessments are done automatically, without human intervention. "
                    "Floods and volcanic eruptions are currently manually introduced. "
                    "Research and development is continuous to improve the global "
                    "monitoring.",
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

                assert_files_same(
                    join(fixtures_dir, "gdacs_rss_information.csv"),
                    join(tempdir, "gdacs_rss_information.csv"),
                )
