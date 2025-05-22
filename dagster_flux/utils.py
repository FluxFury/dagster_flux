from flux_orm.models.models import Match
from typing import Any
import asyncio


def check_news_relevance(formatted_news: dict[str, Any]) -> bool:
    """Checks if the keywords are present in the summary of the news"""
    keywords = formatted_news.get("keywords")
    for keyword in keywords:
        if keyword in formatted_news.get("text"):
            return True
    return False


def prepare_match_to_llm(match: Match) -> dict[str, Any]:
    match_to_llm = {"match_id": match.match_id, "match_teams": []}
    for team in match.match_teams:
        team_members = [
            (member.nickname, member.name, member.description)
            for member in team.members
        ]
        match_to_llm["match_teams"].append((
            team.pretty_name,
            team.description,
            team_members,
        ))

    return match_to_llm


def get_semaphore():
    return asyncio.Semaphore(10)


import hashlib, json


def _make_run_key(batch: list[dict]) -> str:
    # Any deterministic, short hash will work
    digest = hashlib.md5(
        json.dumps([m["match_id"] for m in batch], sort_keys=True).encode()
    ).hexdigest()
    return f"match_batch:{len(batch)}:{digest}"
