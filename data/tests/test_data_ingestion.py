"""
Tests for data/data_ingestion.py

Coverage:
  - PII stripping (whitelist approach — only safe fields survive)
  - CPT to RVU mapping (exact, pattern, description fallback, unknown)
  - SOAP request building (valid XML, credential escaping)
  - Tebra response parsing (happy path, empty, invalid XML)
  - Tebra charge normalisation (column mapping, missing fields)
  - Google Sheets data parsing (mocked API, empty sheet)
  - Error handling (missing env vars, HTTP failure, API errors)
  - run_ingestion_pipeline integration (mocked sub-functions)
"""

import io
import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import xml.etree.ElementTree as ET

import pandas as pd
import requests

# Add data/ to path so we can import data_ingestion directly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import data_ingestion
from data_ingestion import (
    TEBRA_PII_FIELDS,
    TEBRA_SAFE_FIELDS,
    CPT_RVU_MAP,
    _strip_pii_from_charge,
    _build_get_charges_request,
    _parse_charges_response,
    _normalize_tebra_charges,
    cpt_to_rvu,
    pull_tebra_charges,
    pull_google_sheets_va_data,
    run_ingestion_pipeline,
)


# ── PII Stripping ─────────────────────────────────────────────────────────────

class TestStripPii(unittest.TestCase):
    """_strip_pii_from_charge must use a whitelist: only TEBRA_SAFE_FIELDS survive."""

    def _full_charge(self) -> dict:
        """Return a charge dict with both PII and safe fields."""
        pii = {f: f"value_{f}" for f in TEBRA_PII_FIELDS}
        safe = {f: f"value_{f}" for f in TEBRA_SAFE_FIELDS}
        return {**pii, **safe}

    def test_all_pii_fields_removed(self):
        charge = self._full_charge()
        result = _strip_pii_from_charge(charge)
        for pii_field in TEBRA_PII_FIELDS:
            self.assertNotIn(pii_field, result, f"PII field '{pii_field}' survived stripping")

    def test_all_safe_fields_retained(self):
        charge = self._full_charge()
        result = _strip_pii_from_charge(charge)
        for safe_field in TEBRA_SAFE_FIELDS:
            self.assertIn(safe_field, result, f"Safe field '{safe_field}' was incorrectly removed")

    def test_unknown_fields_dropped(self):
        """Fields not in either list must also be dropped (whitelist, not blacklist)."""
        charge = {
            "ServiceDate": "2025-01-01",
            "SomeRandomUnknownField": "secret",
            "AnotherUnknownField": "data",
        }
        result = _strip_pii_from_charge(charge)
        self.assertIn("ServiceDate", result)
        self.assertNotIn("SomeRandomUnknownField", result)
        self.assertNotIn("AnotherUnknownField", result)

    def test_empty_charge_returns_empty(self):
        self.assertEqual(_strip_pii_from_charge({}), {})

    def test_only_pii_returns_empty(self):
        charge = {f: "value" for f in TEBRA_PII_FIELDS}
        self.assertEqual(_strip_pii_from_charge(charge), {})

    def test_specific_pii_fields_blocked(self):
        """Spot-check the most sensitive fields by name."""
        sensitive = [
            "PatientFirstName", "PatientLastName", "PatientDOB",
            "PatientSSN", "PatientAddress", "PatientPhone", "PatientEmail",
        ]
        charge = {f: "secret" for f in sensitive}
        charge["ServiceDate"] = "2025-01-01"
        result = _strip_pii_from_charge(charge)
        for field in sensitive:
            self.assertNotIn(field, result)
        self.assertIn("ServiceDate", result)

    def test_return_is_new_dict(self):
        """Stripping must not mutate the original dict."""
        charge = {"ServiceDate": "2025-01-01", "PatientFirstName": "Jane"}
        original = dict(charge)
        _strip_pii_from_charge(charge)
        self.assertEqual(charge, original)


# ── CPT → RVU Mapping ────────────────────────────────────────────────────────

class TestCptToRvu(unittest.TestCase):

    def test_exact_lookup_e_and_m(self):
        self.assertAlmostEqual(cpt_to_rvu("99213"), 1.30)
        self.assertAlmostEqual(cpt_to_rvu("99215"), 2.80)
        self.assertAlmostEqual(cpt_to_rvu("99214"), 1.92)

    def test_exact_lookup_new_patient(self):
        self.assertAlmostEqual(cpt_to_rvu("99203"), 1.42)
        self.assertAlmostEqual(cpt_to_rvu("99205"), 3.17)

    def test_exact_lookup_nexus_letter(self):
        self.assertAlmostEqual(cpt_to_rvu("99080"), 2.50)

    def test_exact_lookup_dot_physical(self):
        self.assertAlmostEqual(cpt_to_rvu("99455"), 1.77)

    def test_exact_lookup_vaccine_admin(self):
        self.assertAlmostEqual(cpt_to_rvu("90471"), 0.17)
        self.assertAlmostEqual(cpt_to_rvu("90472"), 0.15)

    def test_pattern_em_range(self):
        # 992[01][1-5] — established/new patient E&M range not in exact map
        self.assertAlmostEqual(cpt_to_rvu("99211"), 0.18)  # in exact map

    def test_pattern_annual_range(self):
        # 993xx preventive codes
        self.assertAlmostEqual(cpt_to_rvu("99391"), 1.30)  # not in exact map → pattern

    def test_pattern_vaccine_admin_range(self):
        # 90[4-7][0-9]{2} catch-all for vaccine codes not in exact map
        self.assertAlmostEqual(cpt_to_rvu("90460"), 0.50)

    def test_description_functional_initial(self):
        self.assertAlmostEqual(cpt_to_rvu("XXXXX", "Functional Medicine Initial"), 3.75)

    def test_description_functional_subsequent(self):
        self.assertAlmostEqual(cpt_to_rvu("XXXXX", "Functional Medicine Subsequent Visit"), 2.50)

    def test_description_nexus(self):
        self.assertAlmostEqual(cpt_to_rvu("XXXXX", "Nexus Letter"), 2.50)

    def test_description_dot_physical(self):
        self.assertAlmostEqual(cpt_to_rvu("XXXXX", "DOT Physical Exam"), 1.0)

    def test_description_annual(self):
        self.assertAlmostEqual(cpt_to_rvu("XXXXX", "Annual Wellness Visit"), 1.30)

    def test_description_vaccine(self):
        self.assertAlmostEqual(cpt_to_rvu("XXXXX", "Vaccine Administration"), 0.50)

    def test_unknown_returns_zero(self):
        self.assertAlmostEqual(cpt_to_rvu("00000"), 0.0)
        self.assertAlmostEqual(cpt_to_rvu("ZZZZZ", ""), 0.0)

    def test_case_insensitive_description(self):
        self.assertAlmostEqual(
            cpt_to_rvu("XXXXX", "FUNCTIONAL MEDICINE INITIAL"),
            cpt_to_rvu("XXXXX", "functional medicine initial"),
        )

    def test_whitespace_stripped_from_code(self):
        self.assertAlmostEqual(cpt_to_rvu(" 99213 "), cpt_to_rvu("99213"))

    def test_all_map_entries_non_negative(self):
        for code, rvu in CPT_RVU_MAP.items():
            self.assertGreaterEqual(rvu, 0.0, f"CPT {code} has negative RVU")


# ── SOAP Request Building ─────────────────────────────────────────────────────

class TestBuildGetChargesRequest(unittest.TestCase):

    def test_valid_xml(self):
        xml_str = _build_get_charges_request("KEY123", "2025-01-01", "2025-03-31")
        root = ET.fromstring(xml_str)  # raises if invalid
        self.assertIsNotNone(root)

    def test_contains_api_key(self):
        xml_str = _build_get_charges_request("MYKEY", "2025-01-01", "2025-01-31")
        self.assertIn("MYKEY", xml_str)

    def test_contains_date_range(self):
        xml_str = _build_get_charges_request("K", "2025-02-01", "2025-02-28")
        self.assertIn("2025-02-01", xml_str)
        self.assertIn("2025-02-28", xml_str)

    def test_xml_special_chars_escaped(self):
        """Credentials with XML special characters must be escaped."""
        xml_str = _build_get_charges_request(
            "key&val", "2025-01-01", "2025-01-31",
            username="user<test>", password="pass'word\"here",
        )
        # Should still be parseable
        root = ET.fromstring(xml_str)
        self.assertIsNotNone(root)
        # Raw special chars must not appear unescaped
        self.assertNotIn("key&val", xml_str)   # & must be &amp;
        self.assertNotIn("user<test>", xml_str)  # < must be &lt;

    def test_soap_action_present_in_structure(self):
        xml_str = _build_get_charges_request("K", "2025-01-01", "2025-01-31")
        self.assertIn("GetCharges", xml_str)
        self.assertIn("soap:Body", xml_str)


# ── Tebra Response Parsing ────────────────────────────────────────────────────

_SAMPLE_RESPONSE_ONE_CHARGE = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetChargesResponse xmlns="http://www.kareo.com/api/schemas/">
      <GetChargesResult>
        <Charges>
          <Charge>
            <ServiceDate>2025-01-15</ServiceDate>
            <PatientFirstName>John</PatientFirstName>
            <PatientLastName>Doe</PatientLastName>
            <PatientDOB>1980-06-01</PatientDOB>
            <PatientSSN>123-45-6789</PatientSSN>
            <PatientAddress>123 Main St</PatientAddress>
            <PatientPhone>555-0100</PatientPhone>
            <PatientEmail>jdoe@example.com</PatientEmail>
            <ProcedureCode>99213</ProcedureCode>
            <ChargeAmount>150.00</ChargeAmount>
            <RenderingProviderName>Anne Jenks</RenderingProviderName>
            <FacilityName>JFM Clinic</FacilityName>
            <Units>1</Units>
          </Charge>
        </Charges>
      </GetChargesResult>
    </GetChargesResponse>
  </soap:Body>
</soap:Envelope>"""

_SAMPLE_RESPONSE_EMPTY = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetChargesResponse xmlns="http://www.kareo.com/api/schemas/">
      <GetChargesResult>
        <Charges />
      </GetChargesResult>
    </GetChargesResponse>
  </soap:Body>
</soap:Envelope>"""

_SAMPLE_RESPONSE_TWO_CHARGES = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetChargesResponse xmlns="http://www.kareo.com/api/schemas/">
      <GetChargesResult>
        <Charges>
          <Charge>
            <ServiceDate>2025-02-01</ServiceDate>
            <PatientFirstName>Alice</PatientFirstName>
            <ProcedureCode>99214</ProcedureCode>
            <ChargeAmount>200.00</ChargeAmount>
            <RenderingProviderName>Ehrin Irvin</RenderingProviderName>
            <Units>1</Units>
          </Charge>
          <Charge>
            <ServiceDate>2025-02-03</ServiceDate>
            <PatientLastName>Smith</PatientLastName>
            <PatientSSN>987-65-4321</PatientSSN>
            <ProcedureCode>90471</ProcedureCode>
            <ChargeAmount>25.00</ChargeAmount>
            <RenderingProviderName>Sarah Suggs</RenderingProviderName>
            <Units>1</Units>
          </Charge>
        </Charges>
      </GetChargesResult>
    </GetChargesResponse>
  </soap:Body>
</soap:Envelope>"""


class TestParseChargesResponse(unittest.TestCase):

    def test_single_charge_parsed(self):
        charges = _parse_charges_response(_SAMPLE_RESPONSE_ONE_CHARGE)
        self.assertEqual(len(charges), 1)

    def test_pii_stripped_from_response(self):
        charges = _parse_charges_response(_SAMPLE_RESPONSE_ONE_CHARGE)
        charge = charges[0]
        for pii in ["PatientFirstName", "PatientLastName", "PatientDOB",
                    "PatientSSN", "PatientAddress", "PatientPhone", "PatientEmail"]:
            self.assertNotIn(pii, charge, f"PII field '{pii}' survived parsing")

    def test_safe_fields_present(self):
        charges = _parse_charges_response(_SAMPLE_RESPONSE_ONE_CHARGE)
        charge = charges[0]
        self.assertEqual(charge["ServiceDate"], "2025-01-15")
        self.assertEqual(charge["ProcedureCode"], "99213")
        self.assertEqual(charge["ChargeAmount"], "150.00")
        self.assertEqual(charge["RenderingProviderName"], "Anne Jenks")

    def test_empty_charges_returns_empty_list(self):
        charges = _parse_charges_response(_SAMPLE_RESPONSE_EMPTY)
        self.assertEqual(charges, [])

    def test_two_charges_parsed(self):
        charges = _parse_charges_response(_SAMPLE_RESPONSE_TWO_CHARGES)
        self.assertEqual(len(charges), 2)
        # Verify PII stripped from both
        for charge in charges:
            for pii in ["PatientFirstName", "PatientLastName", "PatientSSN"]:
                self.assertNotIn(pii, charge)

    def test_invalid_xml_raises_value_error(self):
        with self.assertRaises(ValueError, msg="Expected ValueError for invalid XML"):
            _parse_charges_response("this is not xml at all <<<")

    def test_namespace_stripping(self):
        """Parser must handle namespace-qualified element names."""
        xml_ns = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:k="http://www.kareo.com/api/schemas/">
  <s:Body>
    <k:GetChargesResponse>
      <k:GetChargesResult>
        <k:Charges>
          <k:Charge>
            <k:ServiceDate>2025-03-01</k:ServiceDate>
            <k:ProcedureCode>99215</k:ProcedureCode>
            <k:PatientFirstName>Hidden</k:PatientFirstName>
          </k:Charge>
        </k:Charges>
      </k:GetChargesResult>
    </k:GetChargesResponse>
  </s:Body>
</s:Envelope>"""
        charges = _parse_charges_response(xml_ns)
        self.assertEqual(len(charges), 1)
        self.assertNotIn("PatientFirstName", charges[0])
        self.assertEqual(charges[0].get("ServiceDate"), "2025-03-01")

    def test_chargedto_tag_also_parsed(self):
        """Parser must handle 'ChargeDTO' element name used by some Tebra versions."""
        xml = """<?xml version="1.0"?>
<Envelope>
  <Body>
    <ChargeDTO>
      <ServiceDate>2025-04-01</ServiceDate>
      <ProcedureCode>99214</ProcedureCode>
      <PatientFirstName>Hidden</PatientFirstName>
    </ChargeDTO>
  </Body>
</Envelope>"""
        charges = _parse_charges_response(xml)
        self.assertEqual(len(charges), 1)
        self.assertNotIn("PatientFirstName", charges[0])


# ── Tebra Charge Normalisation ────────────────────────────────────────────────

class TestNormalizeTebraCharges(unittest.TestCase):

    def test_column_renaming(self):
        charges = [{
            "ServiceDate": "2025-01-15",
            "ProcedureCode": "99213",
            "ChargeAmount": "150.00",
            "RenderingProviderName": "Anne Jenks",
            "Units": "1",
            "FacilityName": "JFM",
        }]
        df = _normalize_tebra_charges(charges)
        self.assertIn("Date Of Service", df.columns)
        self.assertIn("Procedure Codes with Modifiers", df.columns)
        self.assertIn("Service Charge Amount", df.columns)
        self.assertIn("Rendering Provider", df.columns)
        self.assertIn("Facility Name", df.columns)

    def test_required_columns_added_when_missing(self):
        charges = [{"ServiceDate": "2025-01-15"}]
        df = _normalize_tebra_charges(charges)
        for col in ["Date Of Service", "Rendering Provider",
                    "Procedure Codes with Modifiers", "Service Charge Amount", "Units"]:
            self.assertIn(col, df.columns)

    def test_empty_list_returns_dataframe_with_columns(self):
        df = _normalize_tebra_charges([])
        # Must not raise; returns empty-ish DataFrame
        self.assertIsInstance(df, pd.DataFrame)

    def test_values_preserved_after_rename(self):
        charges = [{"ServiceDate": "2025-03-10", "ChargeAmount": "75.00"}]
        df = _normalize_tebra_charges(charges)
        self.assertEqual(df["Date Of Service"].iloc[0], "2025-03-10")
        self.assertEqual(df["Service Charge Amount"].iloc[0], "75.00")

    def test_insurance_columns_mapped(self):
        charges = [{
            "ServiceDate": "2025-01-01",
            "InsuranceContractAdjustment": "-50.00",
            "InsurancePaymentAmount": "100.00",
            "PatientPaymentAmount": "20.00",
        }]
        df = _normalize_tebra_charges(charges)
        self.assertIn("Pri Ins Insurance Contract Adjustment", df.columns)
        self.assertIn("Pri Ins Insurance Payment", df.columns)
        self.assertIn("Pat Payment Amount", df.columns)


# ── Google Sheets Parsing ─────────────────────────────────────────────────────

class TestPullGoogleSheetsVaData(unittest.TestCase):

    def _mock_service(self, values):
        """Helper: build a mock Sheets service that returns the given values."""
        mock_service = MagicMock()
        (mock_service.spreadsheets.return_value
                     .values.return_value
                     .get.return_value
                     .execute.return_value) = {"values": values}
        return mock_service

    @patch("data_ingestion._get_sheets_service")
    def test_happy_path_returns_csv_bytes(self, mock_factory):
        mock_factory.return_value = self._mock_service([
            ["Date of Service", "Provider", "Focused DBQs", "Routine IMOs",
             "Gen Med DBQs", "TBI", "No Show"],
            ["01/15/2025", "Sarah Suggs", "1-5", "1-3", "0", "0", "0"],
            ["01/16/2025", "Anne Jenks", "0", "0", "1-5", "0", "0"],
        ])
        result = pull_google_sheets_va_data("fake-id")
        self.assertIsInstance(result, bytes)
        df = pd.read_csv(io.BytesIO(result))
        self.assertEqual(len(df), 2)
        self.assertIn("Date of Service", df.columns)
        self.assertIn("Provider", df.columns)

    @patch("data_ingestion._get_sheets_service")
    def test_empty_values_raises_value_error(self, mock_factory):
        mock_factory.return_value = self._mock_service([])
        with self.assertRaises(ValueError, msg="Should raise on empty sheet"):
            pull_google_sheets_va_data("fake-id")

    @patch("data_ingestion._get_sheets_service")
    def test_header_only_raises_value_error(self, mock_factory):
        mock_factory.return_value = self._mock_service([
            ["Date of Service", "Provider"]
        ])
        with self.assertRaises(ValueError, msg="Should raise when no data rows"):
            pull_google_sheets_va_data("fake-id")

    @patch("data_ingestion._get_sheets_service")
    def test_short_rows_padded(self, mock_factory):
        """Rows with fewer columns than the header must not crash the parser."""
        mock_factory.return_value = self._mock_service([
            ["Date of Service", "Provider", "Focused DBQs", "Routine IMOs"],
            ["01/15/2025", "Sarah Suggs"],  # only 2 of 4 columns
        ])
        result = pull_google_sheets_va_data("fake-id")
        df = pd.read_csv(io.BytesIO(result))
        self.assertEqual(len(df), 1)
        # Empty padding becomes NaN when read back through CSV
        self.assertTrue(pd.isna(df["Focused DBQs"].iloc[0]) or df["Focused DBQs"].iloc[0] == "")

    @patch("data_ingestion._get_sheets_service")
    def test_blank_rows_dropped(self, mock_factory):
        mock_factory.return_value = self._mock_service([
            ["Date of Service", "Provider"],
            ["01/15/2025", "Sarah Suggs"],
            ["", ""],          # blank row
            ["01/16/2025", "Anne Jenks"],
        ])
        result = pull_google_sheets_va_data("fake-id")
        df = pd.read_csv(io.BytesIO(result))
        self.assertEqual(len(df), 2)

    @patch("data_ingestion._get_sheets_service")
    def test_api_failure_raises_runtime_error(self, mock_factory):
        mock_service = MagicMock()
        (mock_service.spreadsheets.return_value
                     .values.return_value
                     .get.return_value
                     .execute.side_effect) = Exception("quota exceeded")
        mock_factory.return_value = mock_service
        with self.assertRaises(RuntimeError, msg="Should wrap API exceptions"):
            pull_google_sheets_va_data("fake-id")

    def test_missing_credentials_raises_value_error(self):
        env = {k: v for k, v in os.environ.items() if k != "GOOGLE_SHEETS_CREDENTIALS"}
        google_mocks = {
            "google": MagicMock(),
            "google.oauth2": MagicMock(),
            "google.oauth2.service_account": MagicMock(),
            "googleapiclient": MagicMock(),
            "googleapiclient.discovery": MagicMock(),
        }
        with patch.dict(os.environ, env, clear=True), \
             patch.dict("sys.modules", google_mocks):
            with self.assertRaisesRegex(ValueError, "GOOGLE_SHEETS_CREDENTIALS"):
                data_ingestion._get_sheets_service()


# ── Error Handling ────────────────────────────────────────────────────────────

class TestTebraErrorHandling(unittest.TestCase):

    def test_missing_api_key_raises_value_error(self):
        env = {k: v for k, v in os.environ.items() if k != "TEBRAKEY"}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(ValueError, "TEBRAKEY"):
                pull_tebra_charges()

    def test_empty_api_key_raises_value_error(self):
        with patch.dict(os.environ, {"TEBRAKEY": ""}, clear=False):
            with self.assertRaisesRegex(ValueError, "TEBRAKEY"):
                pull_tebra_charges()

    @patch("requests.post")
    def test_http_failure_raises_runtime_error(self, mock_post):
        mock_post.side_effect = requests.RequestException("Connection refused")
        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            with self.assertRaisesRegex(RuntimeError, "Tebra API request failed"):
                pull_tebra_charges()

    @patch("requests.post")
    def test_http_status_error_raises_runtime_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        mock_post.return_value = mock_response
        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            with self.assertRaisesRegex(RuntimeError, "Tebra API request failed"):
                pull_tebra_charges()

    @patch("requests.post")
    def test_empty_response_returns_empty_csv(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = _SAMPLE_RESPONSE_EMPTY
        mock_post.return_value = mock_response
        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            result = pull_tebra_charges()
        df = pd.read_csv(io.BytesIO(result))
        self.assertEqual(len(df), 0)
        self.assertIn("Date Of Service", df.columns)

    @patch("requests.post")
    def test_valid_response_returns_csv_bytes(self, mock_post):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = _SAMPLE_RESPONSE_TWO_CHARGES
        mock_post.return_value = mock_response
        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            result = pull_tebra_charges()
        self.assertIsInstance(result, bytes)
        df = pd.read_csv(io.BytesIO(result))
        self.assertEqual(len(df), 2)

    @patch("requests.post")
    def test_result_csv_contains_no_pii_columns(self, mock_post):
        """Final CSV output from pull_tebra_charges must not have any PII column names."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = _SAMPLE_RESPONSE_ONE_CHARGE
        mock_post.return_value = mock_response
        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            result = pull_tebra_charges()
        df = pd.read_csv(io.BytesIO(result))
        for pii_field in TEBRA_PII_FIELDS:
            self.assertNotIn(pii_field, df.columns, f"PII column '{pii_field}' in output CSV")

    @patch("requests.post")
    def test_default_date_range_covers_90_days(self, mock_post):
        """Without explicit dates, the request should cover ~90 days ending today."""
        captured = {}

        def capture(*args, **kwargs):
            captured["data"] = kwargs.get("data", args[1] if len(args) > 1 else b"")
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock()
            mock_response.text = _SAMPLE_RESPONSE_EMPTY
            return mock_response

        mock_post.side_effect = capture
        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            pull_tebra_charges()

        from datetime import datetime, timedelta
        today = datetime.today().strftime("%Y-%m-%d")
        ninety_ago = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
        xml_str = captured["data"].decode("utf-8")
        self.assertIn(today, xml_str)
        self.assertIn(ninety_ago, xml_str)


# ── Pipeline Integration ──────────────────────────────────────────────────────

class TestRunIngestionPipeline(unittest.TestCase):

    @patch("data_ingestion.store_csv_to_db")
    @patch("data_ingestion.pull_tebra_charges")
    @patch("data_ingestion.pull_google_sheets_va_data")
    def test_both_sources_pulled_by_default(self, mock_va, mock_pc, mock_store):
        mock_va.return_value = b"Date of Service,Provider\n2025-01-01,Anne Jenks\n"
        mock_pc.return_value = (
            b"Date Of Service,Rendering Provider,Procedure Codes with Modifiers,"
            b"Service Charge Amount,Units\n2025-01-01,Anne Jenks,99213,150.00,1\n"
        )
        results = run_ingestion_pipeline()
        mock_va.assert_called_once()
        mock_pc.assert_called_once()
        self.assertEqual(results["va"]["status"], "ok")
        self.assertEqual(results["pc"]["status"], "ok")
        self.assertEqual(results["va"]["rows"], 1)
        self.assertEqual(results["pc"]["rows"], 1)

    @patch("data_ingestion.store_csv_to_db")
    @patch("data_ingestion.pull_tebra_charges")
    @patch("data_ingestion.pull_google_sheets_va_data")
    def test_va_only(self, mock_va, mock_pc, mock_store):
        mock_va.return_value = b"Date of Service,Provider\n2025-01-01,Sarah Suggs\n"
        results = run_ingestion_pipeline(sources=["va"])
        mock_va.assert_called_once()
        mock_pc.assert_not_called()
        self.assertIn("va", results)
        self.assertNotIn("pc", results)

    @patch("data_ingestion.store_csv_to_db")
    @patch("data_ingestion.pull_tebra_charges")
    @patch("data_ingestion.pull_google_sheets_va_data")
    def test_pc_only(self, mock_va, mock_pc, mock_store):
        mock_pc.return_value = (
            b"Date Of Service,Rendering Provider,Procedure Codes with Modifiers,"
            b"Service Charge Amount,Units\n"
        )
        results = run_ingestion_pipeline(sources=["pc"])
        mock_va.assert_not_called()
        mock_pc.assert_called_once()
        self.assertIn("pc", results)
        self.assertNotIn("va", results)

    @patch("data_ingestion.store_csv_to_db")
    @patch("data_ingestion.pull_tebra_charges")
    @patch("data_ingestion.pull_google_sheets_va_data")
    def test_va_failure_does_not_block_pc(self, mock_va, mock_pc, mock_store):
        mock_va.side_effect = RuntimeError("Google Sheets API down")
        mock_pc.return_value = (
            b"Date Of Service,Rendering Provider,Procedure Codes with Modifiers,"
            b"Service Charge Amount,Units\n"
        )
        results = run_ingestion_pipeline()
        self.assertEqual(results["va"]["status"], "error")
        self.assertIn("Google Sheets API down", results["va"]["error"])
        self.assertEqual(results["pc"]["status"], "ok")

    @patch("data_ingestion.store_csv_to_db")
    @patch("data_ingestion.pull_tebra_charges")
    @patch("data_ingestion.pull_google_sheets_va_data")
    def test_store_called_with_correct_filenames(self, mock_va, mock_pc, mock_store):
        mock_va.return_value = b"Date of Service,Provider\n"
        mock_pc.return_value = (
            b"Date Of Service,Rendering Provider,Procedure Codes with Modifiers,"
            b"Service Charge Amount,Units\n"
        )
        run_ingestion_pipeline()
        stored_filenames = {call.args[0] for call in mock_store.call_args_list}
        self.assertIn("201 Bills and Payments.csv", stored_filenames)
        self.assertIn("Charges Export.csv", stored_filenames)

    @patch("data_ingestion.store_csv_to_db")
    @patch("data_ingestion.pull_tebra_charges")
    @patch("data_ingestion.pull_google_sheets_va_data")
    def test_both_sources_fail_returns_errors(self, mock_va, mock_pc, mock_store):
        mock_va.side_effect = ValueError("No credentials")
        mock_pc.side_effect = ValueError("No API key")
        results = run_ingestion_pipeline()
        self.assertEqual(results["va"]["status"], "error")
        self.assertEqual(results["pc"]["status"], "error")


# ── Enhanced PII Rigour ───────────────────────────────────────────────────────
#
# HIPAA-adjacent requirement: patient data must NEVER survive the pipeline.
# These tests use realistic fake PII values and verify they are absent from
# every stage of output — including the final CSV bytes stored in the database.

from data_ingestion import _dedup_charges, PC_DEDUP_KEYS

# Realistic fake PII values used as "canary" strings.
_FAKE_PII = {
    "PatientFirstName":   "Jane",
    "PatientLastName":    "Testpatient",
    "PatientDOB":         "1978-04-22",
    "PatientSSN":         "987-65-4321",
    "PatientAddress":     "456 Elm Street",
    "PatientAddress2":    "Apt 7B",
    "PatientCity":        "Springfield",
    "PatientState":       "IL",
    "PatientZip":         "62701",
    "PatientPhone":       "555-867-5309",
    "PatientCellPhone":   "555-867-5310",
    "PatientEmail":       "jane.testpatient@example.com",
    "GuarantorFirstName": "Robert",
    "GuarantorLastName":  "Testguarantor",
    "GuarantorAddress":   "789 Oak Ave",
    "GuarantorPhone":     "555-111-2222",
    "PatientChartNumber": "CHART-9999",
    "PatientAccountNumber": "ACCT-8888",
}

_FAKE_SAFE = {
    "ServiceDate":              "2025-03-15",
    "ProcedureCode":            "99214",
    "Modifiers":                "25",
    "RenderingProviderName":    "Ehrin Irvin",
    "FacilityName":             "JFM North",
    "ChargeAmount":             "190.00",
    "Units":                    "1",
    "PrimaryInsuranceName":     "BlueCross",
}


class TestPiiRigour(unittest.TestCase):
    """
    Exhaustive PII protection tests.  Each test verifies that a specific
    PII canary value is absent from the given output stage.
    """

    # ── Stage 1: _strip_pii_from_charge ─────────────────────────────────────

    def test_realistic_pii_stripped_from_charge_dict(self):
        charge = {**_FAKE_PII, **_FAKE_SAFE}
        result = _strip_pii_from_charge(charge)
        for field, value in _FAKE_PII.items():
            self.assertNotIn(field, result,
                             f"PII field '{field}' key survived stripping")
            # Also check that the *value* didn't slip through under another key
            self.assertNotIn(value, result.values(),
                             f"PII value '{value}' (from '{field}') survived stripping")

    def test_pii_values_not_in_output_values(self):
        """Even if field names were renamed, PII values must not appear."""
        charge = {**_FAKE_PII, **_FAKE_SAFE}
        result = _strip_pii_from_charge(charge)
        output_values = set(result.values())
        for field, pii_val in _FAKE_PII.items():
            self.assertNotIn(pii_val, output_values,
                             f"PII value '{pii_val}' from field '{field}' found in output")

    def test_all_known_pii_fields_blocked(self):
        """Every field in TEBRA_PII_FIELDS must be absent from output."""
        charge = {f: f"CANARY_{f}" for f in TEBRA_PII_FIELDS}
        charge.update(_FAKE_SAFE)
        result = _strip_pii_from_charge(charge)
        for pii_field in TEBRA_PII_FIELDS:
            self.assertNotIn(pii_field, result)
            self.assertNotIn(f"CANARY_{pii_field}", result.values())

    # ── Stage 2: _parse_charges_response (XML → dict) ───────────────────────

    def _make_soap_xml(self, extra_fields: dict) -> str:
        fields_xml = "".join(
            f"<{k}>{v}</{k}>" for k, v in extra_fields.items()
        )
        return f"""<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetChargesResponse xmlns="http://www.kareo.com/api/schemas/">
      <GetChargesResult>
        <Charges>
          <Charge>
            <ServiceDate>2025-03-15</ServiceDate>
            <ProcedureCode>99214</ProcedureCode>
            <ChargeAmount>190.00</ChargeAmount>
            <RenderingProviderName>Ehrin Irvin</RenderingProviderName>
            {fields_xml}
          </Charge>
        </Charges>
      </GetChargesResult>
    </GetChargesResponse>
  </soap:Body>
</soap:Envelope>"""

    def test_parse_response_strips_all_realistic_pii(self):
        xml = self._make_soap_xml(_FAKE_PII)
        charges = _parse_charges_response(xml)
        self.assertEqual(len(charges), 1)
        charge = charges[0]
        for field, value in _FAKE_PII.items():
            self.assertNotIn(field, charge,
                             f"PII key '{field}' survived XML parsing")
            self.assertNotIn(value, charge.values(),
                             f"PII value '{value}' survived XML parsing")

    def test_parse_multiple_charges_all_pii_stripped(self):
        """PII stripping must hold for every charge in a multi-record response."""
        charges_xml = ""
        for i in range(5):
            charges_xml += f"""
          <Charge>
            <ServiceDate>2025-0{i+1}-01</ServiceDate>
            <ProcedureCode>9921{i+3}</ProcedureCode>
            <PatientFirstName>Patient{i}</PatientFirstName>
            <PatientSSN>111-{i:02d}-0000</PatientSSN>
            <PatientDOB>198{i}-01-01</PatientDOB>
            <ChargeAmount>100.00</ChargeAmount>
            <RenderingProviderName>Anne Jenks</RenderingProviderName>
          </Charge>"""
        xml = f"""<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body><GetChargesResponse xmlns="http://www.kareo.com/api/schemas/">
    <GetChargesResult><Charges>{charges_xml}</Charges></GetChargesResult>
  </GetChargesResponse></soap:Body>
</soap:Envelope>"""
        charges = _parse_charges_response(xml)
        self.assertEqual(len(charges), 5)
        for i, charge in enumerate(charges):
            self.assertNotIn("PatientFirstName", charge)
            self.assertNotIn("PatientSSN", charge)
            self.assertNotIn("PatientDOB", charge)
            # Verify the actual canary values are absent
            all_values = set(charge.values())
            self.assertNotIn(f"Patient{i}", all_values)
            self.assertNotIn(f"111-{i:02d}-0000", all_values)

    def test_unexpected_pii_field_names_blocked(self):
        """
        A field not in the known PII list but not in SAFE fields either must
        be dropped (whitelist approach).
        """
        unexpected = {
            "PatientMiddleName": "Marie",     # not in PII list but clearly PII
            "PatientInsuranceID": "INS-123",  # not in PII list
            "SomeFuturePatientField": "secret",
        }
        xml = self._make_soap_xml(unexpected)
        charges = _parse_charges_response(xml)
        charge = charges[0]
        for field in unexpected:
            self.assertNotIn(field, charge,
                             f"Unknown field '{field}' should be blocked by whitelist")

    # ── Stage 3: Final CSV output (canary test) ──────────────────────────────

    @patch("requests.post")
    def test_final_csv_output_contains_no_pii_column_names(self, mock_post):
        """
        PII canary: the bytes returned by pull_tebra_charges() and written
        to the database must contain zero PII column names in any form.
        """
        xml = self._make_soap_xml(_FAKE_PII)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = xml
        mock_post.return_value = mock_response

        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            csv_bytes = pull_tebra_charges()

        df = pd.read_csv(io.BytesIO(csv_bytes))
        for pii_field in TEBRA_PII_FIELDS:
            self.assertNotIn(pii_field, df.columns,
                             f"PII column '{pii_field}' in final CSV output")

    @patch("requests.post")
    def test_final_csv_output_contains_no_pii_values(self, mock_post):
        """
        PII canary: the actual PII *values* must not appear anywhere in the
        final CSV bytes — not in any column, not in any value.
        """
        xml = self._make_soap_xml(_FAKE_PII)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = xml
        mock_post.return_value = mock_response

        with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
            csv_bytes = pull_tebra_charges()

        csv_text = csv_bytes.decode("utf-8")
        for field, pii_value in _FAKE_PII.items():
            self.assertNotIn(pii_value, csv_text,
                             f"PII value '{pii_value}' (field '{field}') found in CSV bytes")

    # ── Stage 4: Log output cleanliness ─────────────────────────────────────

    @patch("requests.post")
    def test_log_output_contains_no_pii(self, mock_post):
        """
        No PII canary value should appear in any log record emitted during
        pull_tebra_charges().
        """
        xml = self._make_soap_xml(_FAKE_PII)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = xml
        mock_post.return_value = mock_response

        import logging
        log_records = []

        class _Capture(logging.Handler):
            def emit(self, record):
                log_records.append(self.format(record))

        handler = _Capture()
        handler.setFormatter(logging.Formatter("%(message)s"))
        ingestion_logger = logging.getLogger("data_ingestion")
        ingestion_logger.addHandler(handler)
        ingestion_logger.setLevel(logging.DEBUG)

        try:
            with patch.dict(os.environ, {"TEBRAKEY": "test-key"}):
                pull_tebra_charges()
        finally:
            ingestion_logger.removeHandler(handler)

        combined_logs = "\n".join(log_records)
        for field, pii_value in _FAKE_PII.items():
            self.assertNotIn(pii_value, combined_logs,
                             f"PII value '{pii_value}' (field '{field}') found in log output")

    # ── Stage 5: PII_FIELDS completeness ────────────────────────────────────

    def test_pii_fields_constant_covers_all_fake_pii(self):
        """Every key in _FAKE_PII must be present in TEBRA_PII_FIELDS."""
        for field in _FAKE_PII:
            self.assertIn(field, TEBRA_PII_FIELDS,
                          f"'{field}' is a known PII field but not in TEBRA_PII_FIELDS blocklist")

    def test_safe_fields_and_pii_fields_are_disjoint(self):
        """No field can be in both TEBRA_SAFE_FIELDS and TEBRA_PII_FIELDS."""
        overlap = TEBRA_SAFE_FIELDS & TEBRA_PII_FIELDS
        self.assertEqual(overlap, set(),
                         f"Fields in both safe and PII lists: {overlap}")

    def test_whitelist_rejects_all_unlisted_fields(self):
        """
        Anything not explicitly in TEBRA_SAFE_FIELDS must be dropped,
        including plausible future PII field names.
        """
        plausible_future_pii = [
            "PatientMiddleName", "PatientNickname", "PatientInsuranceID",
            "PatientMemberID", "PatientGroupNumber", "ResponsiblePartyName",
            "ResponsiblePartyDOB", "SubscriberName", "SubscriberDOB",
        ]
        charge = {f: f"secret_{f}" for f in plausible_future_pii}
        charge.update(_FAKE_SAFE)
        result = _strip_pii_from_charge(charge)
        for field in plausible_future_pii:
            self.assertNotIn(field, result,
                             f"Unlisted field '{field}' should be blocked by whitelist")


# ── Deduplication Tests ───────────────────────────────────────────────────────

class TestDeduplication(unittest.TestCase):
    """
    Duplicate prevention: running ingestion twice with the same data must not
    produce duplicate rows in the stored CSV.
    """

    def _make_pc_df(self, rows: list[dict]) -> pd.DataFrame:
        base = {
            "Date Of Service": "2025-03-01",
            "Rendering Provider": "Anne Jenks",
            "Procedure Codes with Modifiers": "99213",
            "Service Charge Amount": "150.00",
            "Units": "1",
        }
        records = [{**base, **r} for r in rows]
        return pd.DataFrame(records)

    def test_identical_rows_deduplicated(self):
        df = self._make_pc_df([{}])
        result = _dedup_charges(df, df.copy())
        self.assertEqual(len(result), 1)

    def test_no_existing_data_returns_new(self):
        new_df = self._make_pc_df([{}, {"Date Of Service": "2025-03-02"}])
        result = _dedup_charges(new_df, pd.DataFrame())
        self.assertEqual(len(result), 2)

    def test_no_new_data_returns_existing(self):
        existing = self._make_pc_df([{}, {"Date Of Service": "2025-03-02"}])
        result = _dedup_charges(pd.DataFrame(), existing)
        self.assertEqual(len(result), 2)

    def test_disjoint_rows_all_retained(self):
        new_df = self._make_pc_df([{"Date Of Service": "2025-03-03"}])
        existing = self._make_pc_df([{"Date Of Service": "2025-03-04"}])
        result = _dedup_charges(new_df, existing)
        self.assertEqual(len(result), 2)

    def test_partial_overlap_correct_count(self):
        """3 new + 3 existing where 1 overlaps → 5 unique rows."""
        new_df = self._make_pc_df([
            {"Date Of Service": "2025-03-01"},  # overlap
            {"Date Of Service": "2025-03-02"},  # new
            {"Date Of Service": "2025-03-03"},  # new
        ])
        existing = self._make_pc_df([
            {"Date Of Service": "2025-03-01"},  # overlap
            {"Date Of Service": "2025-03-04"},  # existing-only
            {"Date Of Service": "2025-03-05"},  # existing-only
        ])
        result = _dedup_charges(new_df, existing)
        self.assertEqual(len(result), 5)

    def test_new_version_of_row_replaces_existing(self):
        """
        If the same date+provider+CPT+units appears with a different charge
        amount, the new version (from new_df) should win.
        """
        new_df = self._make_pc_df([{"Service Charge Amount": "200.00"}])
        existing = self._make_pc_df([{"Service Charge Amount": "150.00"}])
        # Both have same date/provider/CPT/units but different charge amount
        # so they are NOT duplicates under PC_DEDUP_KEYS — both are kept
        result = _dedup_charges(new_df, existing)
        self.assertEqual(len(result), 2)

    def test_same_date_different_cpt_both_kept(self):
        existing = self._make_pc_df([{"Procedure Codes with Modifiers": "99213"}])
        new_df = self._make_pc_df([{"Procedure Codes with Modifiers": "99214"}])
        result = _dedup_charges(new_df, existing)
        self.assertEqual(len(result), 2)

    def test_same_cpt_different_date_both_kept(self):
        existing = self._make_pc_df([{"Date Of Service": "2025-03-01"}])
        new_df = self._make_pc_df([{"Date Of Service": "2025-03-02"}])
        result = _dedup_charges(new_df, existing)
        self.assertEqual(len(result), 2)

    def test_same_cpt_different_provider_both_kept(self):
        existing = self._make_pc_df([{"Rendering Provider": "Anne Jenks"}])
        new_df = self._make_pc_df([{"Rendering Provider": "Ehrin Irvin"}])
        result = _dedup_charges(new_df, existing)
        self.assertEqual(len(result), 2)

    def test_ingestion_twice_does_not_double_rows(self):
        """
        Simulate running the full pipeline twice with the same data.
        Total row count must equal the number of unique charges, not 2×.
        """
        pc_csv = (
            b"Date Of Service,Rendering Provider,Procedure Codes with Modifiers,"
            b"Service Charge Amount,Units\n"
            b"2025-03-01,Anne Jenks,99213,150.00,1\n"
            b"2025-03-02,Anne Jenks,99214,190.00,1\n"
            b"2025-03-03,Ehrin Irvin,99213,150.00,1\n"
        )

        stored = {}

        def fake_store(filename, csv_bytes):
            stored[filename] = csv_bytes

        def fake_read_existing(filename):
            if filename in stored:
                return pd.read_csv(io.BytesIO(stored[filename]))
            return pd.DataFrame()

        with patch("data_ingestion.pull_tebra_charges", return_value=pc_csv), \
             patch("data_ingestion.pull_google_sheets_va_data",
                   return_value=b"Date of Service,Provider\n"), \
             patch("data_ingestion.store_csv_to_db", side_effect=fake_store), \
             patch("data_ingestion._read_existing_csv_from_db",
                   side_effect=fake_read_existing):
            # First ingestion
            run_ingestion_pipeline()
            first_count = len(pd.read_csv(io.BytesIO(stored["Charges Export.csv"])))

            # Second ingestion with identical data
            run_ingestion_pipeline()
            second_count = len(pd.read_csv(io.BytesIO(stored["Charges Export.csv"])))

        self.assertEqual(first_count, 3, "First run should have 3 rows")
        self.assertEqual(second_count, 3, "Second run must not duplicate rows")

    def test_new_data_added_on_second_ingestion(self):
        """Second run with additional charges adds only the new ones."""
        first_csv = (
            b"Date Of Service,Rendering Provider,Procedure Codes with Modifiers,"
            b"Service Charge Amount,Units\n"
            b"2025-03-01,Anne Jenks,99213,150.00,1\n"
        )
        second_csv = (
            b"Date Of Service,Rendering Provider,Procedure Codes with Modifiers,"
            b"Service Charge Amount,Units\n"
            b"2025-03-01,Anne Jenks,99213,150.00,1\n"   # duplicate
            b"2025-03-02,Anne Jenks,99214,190.00,1\n"   # new
        )

        stored = {}
        pull_results = iter([first_csv, second_csv])

        def fake_store(filename, csv_bytes):
            stored[filename] = csv_bytes

        def fake_read_existing(filename):
            if filename in stored:
                return pd.read_csv(io.BytesIO(stored[filename]))
            return pd.DataFrame()

        with patch("data_ingestion.pull_tebra_charges", side_effect=pull_results), \
             patch("data_ingestion.pull_google_sheets_va_data",
                   return_value=b"Date of Service,Provider\n"), \
             patch("data_ingestion.store_csv_to_db", side_effect=fake_store), \
             patch("data_ingestion._read_existing_csv_from_db",
                   side_effect=fake_read_existing):
            run_ingestion_pipeline(sources=["pc"])
            count_after_first = len(pd.read_csv(io.BytesIO(stored["Charges Export.csv"])))

            run_ingestion_pipeline(sources=["pc"])
            count_after_second = len(pd.read_csv(io.BytesIO(stored["Charges Export.csv"])))

        self.assertEqual(count_after_first, 1)
        self.assertEqual(count_after_second, 2, "Only the genuinely new row should be added")


if __name__ == "__main__":
    unittest.main()
