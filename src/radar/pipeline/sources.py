"""Collect and ingest one named source.

The CLI imports these functions. This module does not import the CLI.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.collectors.clist import ClistCollector
from radar.collectors.codeforces import CodeforcesCollector
from radar.collectors.devfolio import DevfolioCollector
from radar.collectors.devpost import DevpostCollector
from radar.collectors.unstop import UnstopCollector
from radar.models import HealthStatus, Source, SourceKind
from radar.pipeline.ingest import SourceRunResult, ingest_run

_CODEFORCES = "codeforces"
_CLIST = "clist"
_DEVPOST = "devpost"
_UNSTOP = "unstop"
_DEVFOLIO = "devfolio"


async def scan_codeforces(session: Session) -> SourceRunResult:
    collected = await CodeforcesCollector().collect()
    return ingest_run(session, _get_source(session, _CODEFORCES), collected)


async def scan_clist(session: Session) -> SourceRunResult:
    collected = await ClistCollector().collect()
    return ingest_run(session, _get_source(session, _CLIST), collected)


async def scan_devpost(session: Session) -> SourceRunResult:
    collected = await DevpostCollector().collect()
    return ingest_run(session, _get_source(session, _DEVPOST), collected)


async def scan_unstop(session: Session) -> SourceRunResult:
    collected = await UnstopCollector().collect()
    return ingest_run(session, _get_source(session, _UNSTOP), collected)


async def scan_devfolio(session: Session) -> SourceRunResult:
    collected = await DevfolioCollector().collect()
    return ingest_run(session, _get_source(session, _DEVFOLIO), collected)


SCANNERS = {
    _CODEFORCES: scan_codeforces,
    _CLIST: scan_clist,
    _DEVPOST: scan_devpost,
    _UNSTOP: scan_unstop,
    _DEVFOLIO: scan_devfolio,
}


def _get_source(session: Session, name: str) -> Source:
    source = session.scalar(select(Source).where(Source.name == name))
    if source is not None:
        return source
    source = Source(
        kind=SourceKind.API,
        name=name,
        config={},
        enabled=True,
        health_status=HealthStatus.HEALTHY,
        consecutive_failures=0,
    )
    session.add(source)
    session.flush()
    return source
