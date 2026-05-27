import unittest
from types import SimpleNamespace

from openg2p_registry_celery_workers.tasks.ingest_data_worker import (
    _merge_section_records,
    _validate_transformed_data_json,
)


class IngestDataWorkerTests(unittest.TestCase):
    def test_subject_sections_reuse_same_internal_record_id(self):
        subject_section = SimpleNamespace(
            section_register_id="farmer-register",
            is_list=False,
            section_mnemonic="farmer_personal",
        )
        later_subject_section = SimpleNamespace(
            section_register_id="farmer-register",
            is_list=False,
            section_mnemonic="farmer_location",
        )

        first_records = _merge_section_records(
            section=subject_section,
            subject_register_id="farmer-register",
            incoming_records=[{"first_name": "John"}],
            accumulated_records=[],
            accumulated_ids=[],
        )
        second_records = _merge_section_records(
            section=later_subject_section,
            subject_register_id="farmer-register",
            incoming_records=[{"address_line_1": "Village Road"}],
            accumulated_records=first_records,
            accumulated_ids=[first_records[0]["internal_record_id"]],
        )

        self.assertEqual(len(second_records), 1)
        self.assertEqual(
            second_records[0]["internal_record_id"],
            first_records[0]["internal_record_id"],
        )
        self.assertEqual(second_records[0]["first_name"], "John")
        self.assertEqual(second_records[0]["address_line_1"], "Village Road")

    def test_list_sections_append_new_rows_for_same_register(self):
        list_section = SimpleNamespace(
            section_register_id="land-register",
            is_list=True,
            section_mnemonic="land_basic",
        )
        later_list_section = SimpleNamespace(
            section_register_id="land-register",
            is_list=True,
            section_mnemonic="land_more",
        )

        first_records = _merge_section_records(
            section=list_section,
            subject_register_id="farmer-register",
            incoming_records=[{"acreage": 1}, {"acreage": 2}],
            accumulated_records=[],
            accumulated_ids=[],
        )
        second_records = _merge_section_records(
            section=later_list_section,
            subject_register_id="farmer-register",
            incoming_records=[{"soil_type": "clay"}, {"soil_type": "loam"}, {"soil_type": "sand"}],
            accumulated_records=first_records,
            accumulated_ids=[record["internal_record_id"] for record in first_records],
        )

        self.assertEqual(len(second_records), 3)
        self.assertEqual(second_records[0]["internal_record_id"], first_records[0]["internal_record_id"])
        self.assertEqual(second_records[1]["internal_record_id"], first_records[1]["internal_record_id"])
        self.assertEqual(second_records[0]["soil_type"], "clay")
        self.assertEqual(second_records[1]["soil_type"], "loam")
        self.assertEqual(second_records[2]["soil_type"], "sand")

    def test_validate_transformed_data_rejects_unknown_section_mnemonic(self):
        incoming_classified_data = SimpleNamespace(
            intake_form_id="form-1",
            register_id="farmer-register",
        )
        transformed_data = SimpleNamespace(
            transformed_data_json={"unknown_section": [{}]},
        )
        ordered_sections = [
            SimpleNamespace(
                section_mnemonic="farmer_personal",
                section_register_id="farmer-register",
                is_list=False,
            )
        ]

        with self.assertRaisesRegex(ValueError, "not present in intake form"):
            _validate_transformed_data_json(
                incoming_classified_data,
                transformed_data,
                ordered_sections,
            )

    def test_validate_transformed_data_requires_subject_section(self):
        incoming_classified_data = SimpleNamespace(
            intake_form_id="form-1",
            register_id="farmer-register",
        )
        transformed_data = SimpleNamespace(
            transformed_data_json={"land_section": [{"acreage": 1}]},
        )
        ordered_sections = [
            SimpleNamespace(
                section_mnemonic="land_section",
                section_register_id="land-register",
                is_list=True,
            )
        ]

        with self.assertRaisesRegex(ValueError, "must include at least one subject-register section"):
            _validate_transformed_data_json(
                incoming_classified_data,
                transformed_data,
                ordered_sections,
            )


if __name__ == "__main__":
    unittest.main()
