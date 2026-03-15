"""
test_normalizer.py — Unit tests for the OCR normalization layer.

Run with:
    python -m unittest test_normalizer -v
"""

import unittest
from normalizer import normalize, normalize_unit, normalize_name, validate_price, canonicalize_names
from unittest.mock import patch


class TestCanonicalizeNames(unittest.TestCase):

    def test_empty_known_names(self):
        items = [{"english_name": "New Item"}]
        result = canonicalize_names(items, [])
        self.assertEqual(result, items)

    @patch("normalizer._call_gemini_canonicalize")
    def test_canonicalize_applies_mapping(self, mock_gemini):
        # Mock Gemini returns {"Guava Awal": "Guava A-Grade"}
        mock_gemini.return_value = {"Guava Awal": "Guava A-Grade"}

        items = [
            {"english_name": "Guava Awal", "price_1": 100},
            {"english_name": "Apple", "price_1": 200}
        ]
        known_names = ["Guava A-Grade", "Apple"]

        result = canonicalize_names(items, known_names)

        self.assertEqual(result[0]["english_name"], "Guava A-Grade")
        self.assertEqual(result[1]["english_name"], "Apple")
        mock_gemini.assert_called_once_with(["Guava Awal"], known_names)

    @patch("normalizer._call_gemini_canonicalize")
    def test_unmapped_new_names_untouched(self, mock_gemini):
        # Gemini decides the new name is genuinely new, returns no mapping
        mock_gemini.return_value = {}

        items = [{"english_name": "Dragonfruit"}]
        known_names = ["Apple", "Banana"]

        result = canonicalize_names(items, known_names)

        self.assertEqual(result[0]["english_name"], "Dragonfruit")


class TestNormalizeUnit(unittest.TestCase):

    def test_verbose_per_kg(self):
        self.assertEqual(normalize_unit("per kg (or per piece if mentioned)"), "per kg")

    def test_per_piece_variant(self):
        self.assertEqual(normalize_unit("per piece"), "per piece")

    def test_each(self):
        self.assertEqual(normalize_unit("each"), "per piece")

    def test_per_dozen(self):
        self.assertEqual(normalize_unit("per dozen"), "per dozen")
        self.assertEqual(normalize_unit("Dozen"), "per dozen")

    def test_per_40_kg(self):
        self.assertEqual(normalize_unit("40 kg bag"), "per 40 kg")
        self.assertEqual(normalize_unit("per 40 kg"), "per 40 kg")

    def test_maund(self):
        self.assertEqual(normalize_unit("per maund"), "per maund")
        self.assertEqual(normalize_unit("Maund"), "per maund")

    def test_none_defaults_per_kg(self):
        self.assertEqual(normalize_unit(None), "per kg")

    def test_empty_defaults_per_kg(self):
        self.assertEqual(normalize_unit(""), "per kg")

    def test_unknown_defaults_per_kg(self):
        self.assertEqual(normalize_unit("some weird unit"), "per kg")


class TestNormalizeName(unittest.TestCase):

    def test_apple_iranian_variants(self):
        self.assertEqual(normalize_name("Apple Iranian"), "Apple Irani")
        self.assertEqual(normalize_name("apple iran"),    "Apple Irani")
        self.assertEqual(normalize_name("Apple Irani"),   "Apple Irani")

    def test_potato_regular(self):
        self.assertEqual(normalize_name("potato regular"), "Potato")
        self.assertEqual(normalize_name("Potato"),         "Potato")

    def test_tomato_regular(self):
        self.assertEqual(normalize_name("tomato regular"), "Tomato")

    def test_onion_regular(self):
        self.assertEqual(normalize_name("onion regular"), "Onion")

    def test_title_case_fallthrough(self):
        # Unknown name → title-cased
        self.assertEqual(normalize_name("some exotic fruit"), "Some Exotic Fruit")

    def test_none_falls_back_to_urdu(self):
        result = normalize_name(None, urdu_fallback="آم")
        self.assertEqual(result, "آم")

    def test_none_no_urdu_returns_unknown(self):
        result = normalize_name(None, urdu_fallback="")
        self.assertEqual(result, "Unknown")

    def test_strips_per_kg_from_name(self):
        # "(per kg)" embedded in name should be stripped
        result = normalize_name("Banana (per kg)")
        self.assertNotIn("per kg", result.lower())

    def test_strips_extra_whitespace(self):
        result = normalize_name("  Apple   Golden  ")
        self.assertEqual(result, "Apple Golden")


class TestValidatePrice(unittest.TestCase):

    def test_valid_fruits_price(self):
        self.assertEqual(validate_price(150, "fruits"), 150.0)

    def test_valid_poultry_price(self):
        self.assertEqual(validate_price(500, "poultry"), 500.0)

    def test_zero_is_invalid(self):
        self.assertIsNone(validate_price(0, "fruits"))

    def test_negative_is_invalid(self):
        self.assertIsNone(validate_price(-10, "fruits"))

    def test_absurdly_high_price(self):
        self.assertIsNone(validate_price(999_999, "fruits"))

    def test_none_input(self):
        self.assertIsNone(validate_price(None, "fruits"))

    def test_string_number(self):
        # Gemini sometimes returns "120" as a string
        self.assertEqual(validate_price("120", "fruits"), 120.0)

    def test_non_numeric_string(self):
        self.assertIsNone(validate_price("N/A", "fruits"))

    def test_unknown_category_uses_default_bounds(self):
        self.assertEqual(validate_price(5000, "grains"), 5000.0)


class TestNormalizeFull(unittest.TestCase):

    def _make_result(self, items, unit="per kg (or per piece if mentioned)", category="fruits"):
        return {
            "date":     "2026-03-14",
            "category": category,
            "unit":     unit,
            "items":    items,
        }

    def test_unit_is_cleaned(self):
        raw = self._make_result([])
        result = normalize(raw)
        self.assertEqual(result["unit"], "per kg")

    def test_name_is_normalized(self):
        raw = self._make_result([{
            "english_name": "Apple Iranian",
            "urdu_name":    "سیب",
            "price_1":      300,
            "price_2":      280,
        }])
        result = normalize(raw)
        self.assertEqual(result["items"][0]["english_name"], "Apple Irani")

    def test_price_swap_when_p1_lt_p2(self):
        """price_1 should always be >= price_2 (اول = premium quality)."""
        raw = self._make_result([{
            "english_name": "Tomato",
            "urdu_name":    "ٹماٹر",
            "price_1":      50,     # wrong order
            "price_2":      200,
        }])
        result = normalize(raw)
        item = result["items"][0]
        self.assertGreaterEqual(item["price_1"], item["price_2"])
        self.assertEqual(item["price_1"], 200)
        self.assertEqual(item["price_2"], 50)

    def test_invalid_price_set_to_none(self):
        raw = self._make_result([{
            "english_name": "Banana",
            "urdu_name":    "کیلا",
            "price_1":      999_999,  # out of range for fruits
            "price_2":      100,
        }])
        result = normalize(raw)
        self.assertIsNone(result["items"][0]["price_1"])

    def test_duplicate_items_deduplicated(self):
        raw = self._make_result([
            {"english_name": "Mango", "urdu_name": "آم", "price_1": 200, "price_2": 180},
            {"english_name": "mango", "urdu_name": "آم", "price_1": 190, "price_2": 170},  # duplicate
        ])
        result = normalize(raw)
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(len(result["skipped"]), 1)
        self.assertEqual(result["skipped"][0]["reason"], "duplicate")

    def test_null_prices_preserved(self):
        raw = self._make_result([{
            "english_name": "Dates",
            "urdu_name":    "کھجور",
            "price_1":      None,
            "price_2":      None,
        }])
        result = normalize(raw)
        item = result["items"][0]
        self.assertIsNone(item["price_1"])
        self.assertIsNone(item["price_2"])

    def test_category_lowercased(self):
        raw = self._make_result([], category="Fruits")
        result = normalize(raw)
        self.assertEqual(result["category"], "fruits")

    def test_empty_items_list(self):
        raw = self._make_result([])
        result = normalize(raw)
        self.assertEqual(result["items"], [])
        self.assertEqual(result["skipped"], [])


if __name__ == "__main__":
    unittest.main()
