import pytest
# Note: external_source may need to be migrated to source/ module
# from pyodibel.source.external_source import WikipediaSummaries

@pytest.mark.slow
def test_wikipedia_summaries():
    summaries = WikipediaSummaries()
    summary_response = summaries.get("The_Matrix")
    print(summary_response.extract)
