from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from dagster import get_dagster_logger
from typing import Any

llm_chat = ChatOpenAI(
    model="gpt-4o",
    temperature=0.0,
    timeout=600,
    max_retries=5,
).with_structured_output(method="json_mode")

logger = get_dagster_logger()


async def summarize_news_and_extract_keywords(news: str) -> tuple[str, list[str]]:
    system_prompt = """
    You are a helpful assistant that summarizes news articles and extracts keywords for gamblers.
    You should return a JSON object with the following fields:
    - summary: a summary of the news article
    - keywords: a dict of keywords extracted from the news article with the following fields:
        - People: a list of people mentioned in the news article, if a person has a nickname, use the following format: "First name 'nickname' Last name"
        - Organizations: a list of organizations mentioned in the news article
        - Locations: a list of locations mentioned in the news article
        - Events: a list of events mentioned in the news article
        - Objects: a list of objects mentioned in the news article
        - Other: a list of other keywords mentioned in the news article
    If you think that there is no information for gamblers, you should answer with a JSON object with the following fields:
    - summary: "No info"
    - keywords: []
    """
    human_prompt = f"""
    Here is the news article:
    {news}
    """
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=human_prompt),
    ]
    completion = await llm_chat.ainvoke(messages)
    json_response = completion
    summary, keywords = json_response["summary"], json_response["keywords"]
    return summary, keywords


async def determine_match_to_news_relevance(match_news_pair: dict[str, Any]) -> bool:
    match = match_news_pair["match"]
    news = match_news_pair["news"]
    system_prompt = """
    You are a helpful assistant that determines if a sports match is relevant to a news article.
    You should return a JSON object with the following fields:
    - is_relevant: a boolean value indicating if the match is relevant to the news article
    """
    human_prompt = f"""
    Here is the match:
    {match}
    Here is the news:
    {news}
    """
    logger.info(f"Determining if match is relevant to news: {human_prompt}")
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=human_prompt),
    ]
    completion = await llm_chat.ainvoke(messages)
    json_response = completion
    is_relevant = json_response["is_relevant"]
    return is_relevant


async def rank_news_by_llm(
    match_to_llm: dict[str, Any], news_to_llm_list: list[dict[str, Any]]
) -> dict[str, int]:
    rankings = {}

    system_prompt = """
    You are a helpful assistant that ranks news articles by relevance to a sports match.
    You should return a JSON dictionary with the following field:
    - news_id: respective_relevance
    
    respective_relevance is a number between 0 and 100 indicating the relevance of the news article to the sports match
    """
    human_prompt = f"""
    Here is the match:
    {match_to_llm}
    Here are the news:
    {news_to_llm_list}
    """
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=human_prompt),
    ]
    completion = await llm_chat.ainvoke(messages)
    rankings: dict[str, int] = completion
    return rankings
