from dagster import job
from dagster_flux.ops import (
    read_raw_news_from_kafka_op,
    summarize_and_format_news_op,
    store_formatted_news_in_db_op,
    extract_matches_op,
    cross_news_with_matches_op,
    filter_pairs_by_llm_op,
    store_filtered_match_in_news_op,
    rank_news_by_llm_op,
    filter_pairs_by_algorithm_op,
    mark_match_finished_op,
)

@job
def news_processing_job():
    """
    """
    raw_news = read_raw_news_from_kafka_op()
    formatted_news = summarize_and_format_news_op(raw_news)
    stored_formatted_news = store_formatted_news_in_db_op(formatted_news)
    matches = extract_matches_op()
    cross_result = cross_news_with_matches_op(matches, stored_formatted_news)
    filtered_by_algorithm_pairs = filter_pairs_by_algorithm_op(cross_result)
    filtered_by_llm_pairs = filter_pairs_by_llm_op(filtered_by_algorithm_pairs)
    store_filtered_match_in_news_op(filtered_by_llm_pairs)



@job
def ranking_job():
    rank_news_by_llm_op()


@job
def mark_matches_finished_job():
    mark_match_finished_op()