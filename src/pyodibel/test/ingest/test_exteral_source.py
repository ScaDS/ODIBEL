import pytest
from pyodibel.ingest.external_source import WikipediaSummaries

@pytest.mark.slow
def test_wikipedia_summaries():
    summaries = WikipediaSummaries()
    summary_response = summaries.get("The_Matrix")
    print(summary_response.extract)
