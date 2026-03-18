#!/usr/bin/env python3
"""
test_mapreduce.py
Unit tests for the MapReduce mapper, combiner, reducer, top mapper/reducer,
and sentiment mapper/reducer — all runnable locally without a Hadoop cluster.
"""

import unittest
import subprocess
import tempfile
import os


def pipe(cmd: str, stdin: str) -> str:
    """Run a shell pipeline and return stdout as a string."""
    result = subprocess.run(
        cmd, shell=True, input=stdin.encode(), capture_output=True
    )
    return result.stdout.decode().strip()


def run_mapreduce(mapper: str, reducer: str, input_data: str, combiner: str = None) -> str:
    """Simulate a full MapReduce job locally via pipes."""
    if combiner:
        cmd = (
            f"echo '{input_data}' | python3 {mapper} "
            f"| sort | python3 {combiner} "
            f"| sort | python3 {reducer}"
        )
    else:
        cmd = f"echo '{input_data}' | python3 {mapper} | sort | python3 {reducer}"
    return pipe(cmd, "")


# ── Shared test data ─────────────────────────────────────────────────────────
# Format: subreddit \t body_clean \t sentiment_label
SAMPLE_COMMENTS = "\n".join([
    "worldnews\tglobal climate change is a real crisis affecting everyone\tpositive",
    "worldnews\tterrible disaster kills hundreds of people worldwide\tnegative",
    "technology\tartificial intelligence is transforming modern software development\tpositive",
    "technology\tprivacy concerns are rising due to big tech surveillance\tnegative",
    "science\tresearchers discover new treatment for cancer patients today\tpositive",
    "science\tstudy shows alarming increase in microplastics in oceans\tnegative",
    "worldnews\telectioms results spark protests across several countries\tneutral",
    "technology\tnew programming language released by major tech company today\tpositive",
])

MAPPER = "src/mapreduce/mapper.py"
REDUCER = "src/mapreduce/reducer.py"
COMBINER = "src/mapreduce/combiner.py"
TOP_MAPPER = "src/mapreduce/top_mapper.py"
TOP_REDUCER = "src/mapreduce/top_reducer.py"
SENT_MAPPER = "src/mapreduce/sentiment_mapper.py"
SENT_REDUCER = "src/mapreduce/sentiment_reducer.py"


class TestMapper(unittest.TestCase):

    def setUp(self):
        self.output = pipe(f"python3 {MAPPER}", SAMPLE_COMMENTS)

    def test_output_not_empty(self):
        self.assertTrue(len(self.output) > 0, "Mapper produced no output")

    def test_tab_delimited_three_fields(self):
        """Every output line should be: subreddit \t word \t 1"""
        for line in self.output.split("\n"):
            parts = line.split("\t")
            self.assertEqual(len(parts), 3, f"Expected 3 fields, got: {line!r}")
            self.assertEqual(parts[2], "1", f"Third field should be '1', got: {line!r}")

    def test_subreddits_present(self):
        self.assertIn("worldnews", self.output)
        self.assertIn("technology", self.output)
        self.assertIn("science", self.output)

    def test_stopwords_removed(self):
        """Common stopwords should not appear as emitted words."""
        words = [line.split("\t")[1] for line in self.output.split("\n") if line]
        for stopword in ("the", "a", "is", "and", "or", "in", "of"):
            self.assertNotIn(stopword, words, f"Stopword '{stopword}' should be filtered")

    def test_short_words_removed(self):
        """Words of length <= 2 should be filtered out."""
        words = [line.split("\t")[1] for line in self.output.split("\n") if line]
        for word in words:
            self.assertGreater(len(word), 2, f"Short word '{word}' should be filtered")

    def test_known_words_emitted(self):
        """Check a few content words that should survive normalization."""
        self.assertIn("climate", self.output)
        self.assertIn("artificial", self.output)
        self.assertIn("intelligence", self.output)


class TestReducer(unittest.TestCase):

    def setUp(self):
        self.output = run_mapreduce(MAPPER, REDUCER, SAMPLE_COMMENTS)

    def test_output_not_empty(self):
        self.assertTrue(len(self.output) > 0)

    def test_tab_delimited_three_fields(self):
        """Reducer output: subreddit \t word \t count"""
        for line in self.output.split("\n"):
            parts = line.split("\t")
            self.assertEqual(len(parts), 3, f"Expected 3 fields: {line!r}")
            # count should be a positive integer
            self.assertTrue(parts[2].isdigit(), f"Count not integer: {line!r}")
            self.assertGreater(int(parts[2]), 0)

    def test_aggregation_correct(self):
        """Word 'today' appears in technology and science → each should have count >= 1."""
        found = {
            line.split("\t")[0]: int(line.split("\t")[2])
            for line in self.output.split("\n")
            if line and line.split("\t")[1] == "today"
        }
        # 'today' appears in technology and science comments
        self.assertTrue(len(found) >= 1, "Expected 'today' in at least one subreddit")


class TestCombiner(unittest.TestCase):

    def test_combiner_reduces_records(self):
        """Combiner output should have fewer or equal records than raw mapper output."""
        raw_mapper_out = pipe(f"python3 {MAPPER}", SAMPLE_COMMENTS)
        raw_count = len(raw_mapper_out.strip().split("\n"))

        combined_out = pipe(f"python3 {MAPPER} | sort | python3 {COMBINER}", SAMPLE_COMMENTS)
        combined_count = len(combined_out.strip().split("\n"))

        self.assertLessEqual(combined_count, raw_count,
                             "Combiner should reduce or equal record count")

    def test_combiner_output_format(self):
        combined_out = pipe(
            f"python3 {MAPPER} | sort | python3 {COMBINER}", SAMPLE_COMMENTS
        )
        for line in combined_out.split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            self.assertEqual(len(parts), 3, f"Bad combiner output: {line!r}")


class TestFullWordFrequencyPipeline(unittest.TestCase):

    def setUp(self):
        self.output = run_mapreduce(MAPPER, REDUCER, SAMPLE_COMMENTS, combiner=COMBINER)

    def test_word_counts_positive(self):
        for line in self.output.split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            count = int(parts[2])
            self.assertGreater(count, 0)

    def test_subreddits_in_output(self):
        subreddits_in_output = {line.split("\t")[0] for line in self.output.split("\n") if line}
        self.assertIn("worldnews", subreddits_in_output)
        self.assertIn("technology", subreddits_in_output)
        self.assertIn("science", subreddits_in_output)


class TestTopMapperReducer(unittest.TestCase):

    def setUp(self):
        # Build intermediate reducer output first
        self.reducer_out = run_mapreduce(MAPPER, REDUCER, SAMPLE_COMMENTS, combiner=COMBINER)

    def test_top_mapper_adds_prefix(self):
        top_mapper_out = pipe(f"python3 {TOP_MAPPER}", self.reducer_out)
        for line in top_mapper_out.split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            self.assertEqual(len(parts), 3)
            self.assertTrue(
                parts[0].startswith("SUBREDDIT_") or parts[0] == "GLOBAL",
                f"Unexpected key prefix: {parts[0]!r}"
            )

    def test_top_reducer_section_headers(self):
        top_out = pipe(
            f"python3 {TOP_MAPPER} | sort | python3 {TOP_REDUCER}",
            self.reducer_out
        )
        self.assertIn("TOP", top_out, "Top reducer should output section headers")
        self.assertIn("GLOBAL", top_out)

    def test_top_reducer_rankings_are_numeric(self):
        top_out = pipe(
            f"python3 {TOP_MAPPER} | sort | python3 {TOP_REDUCER}",
            self.reducer_out
        )
        for line in top_out.split("\n"):
            if not line or line.startswith("="):
                continue
            parts = line.split("\t")
            if len(parts) == 3:
                self.assertTrue(parts[0].isdigit(), f"Rank should be numeric: {line!r}")
                self.assertGreater(int(parts[2]), 0, "Count should be > 0")


class TestSentimentMapperReducer(unittest.TestCase):

    def setUp(self):
        self.sent_out = pipe(
            f"python3 {SENT_MAPPER} | sort | python3 {SENT_REDUCER}",
            SAMPLE_COMMENTS
        )

    def test_output_not_empty(self):
        self.assertTrue(len(self.sent_out) > 0)

    def test_sentiment_labels_valid(self):
        """All emitted sentiment labels should be positive/neutral/negative."""
        valid = {"positive", "neutral", "negative"}
        for line in self.sent_out.split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            self.assertEqual(len(parts), 3)
            self.assertIn(parts[1], valid, f"Invalid sentiment label: {parts[1]!r}")

    def test_counts_are_positive_integers(self):
        for line in self.sent_out.split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            self.assertTrue(parts[2].isdigit())
            self.assertGreater(int(parts[2]), 0)

    def test_all_subreddits_present(self):
        subreddits_in_output = {line.split("\t")[0] for line in self.sent_out.split("\n") if line}
        self.assertIn("worldnews", subreddits_in_output)
        self.assertIn("technology", subreddits_in_output)
        self.assertIn("science", subreddits_in_output)

    def test_sentiment_count_matches_input(self):
        """Total sentiment count should equal number of valid input lines."""
        total_count = sum(
            int(line.split("\t")[2])
            for line in self.sent_out.split("\n")
            if line
        )
        # 8 sample comments, all with valid sentiment labels
        self.assertEqual(total_count, 8)


class TestLocalPipelineIntegration(unittest.TestCase):
    """End-to-end local simulation of all three MapReduce stages."""

    def test_stage1_word_freq(self):
        out = run_mapreduce(MAPPER, REDUCER, SAMPLE_COMMENTS, combiner=COMBINER)
        self.assertTrue(len(out) > 0)
        # Spot check: 'crisis' appears in worldnews → should be in output
        self.assertIn("crisis", out)

    def test_stage2_top_words(self):
        stage1_out = run_mapreduce(MAPPER, REDUCER, SAMPLE_COMMENTS, combiner=COMBINER)
        top_out = pipe(
            f"python3 {TOP_MAPPER} | sort | python3 {TOP_REDUCER}",
            stage1_out
        )
        self.assertTrue(len(top_out) > 0)

    def test_stage3_sentiment_counts(self):
        out = pipe(
            f"python3 {SENT_MAPPER} | sort | python3 {SENT_REDUCER}",
            SAMPLE_COMMENTS
        )
        self.assertTrue(len(out) > 0)
        total = sum(int(l.split("\t")[2]) for l in out.split("\n") if l)
        self.assertEqual(total, 8)


if __name__ == "__main__":
    unittest.main(verbosity=2)
