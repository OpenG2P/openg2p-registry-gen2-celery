import logging
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.models import G2PScoreComputeQueue
from openg2p_registry_core.models.g2p_score_compute_queue import (
    ProcessStatusEnum as ScoreComputeStatusEnum,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="score_compute_beat_producer")
def score_compute_beat_producer():
    """
    Beat producer that finds PENDING score-computation queue items
    and queues them to the score-compute worker.
    """
    _logger.info("Checking for pending score compute requests")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        pending_queue_items: List[G2PScoreComputeQueue] = (
            session.execute(
                select(G2PScoreComputeQueue).where(
                    G2PScoreComputeQueue.compute_status
                    == ScoreComputeStatusEnum.PENDING.value
                ).limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(
            "Found %s PENDING score compute requests",
            len(pending_queue_items),
        )

        for pending_queue_item in pending_queue_items:
            _logger.info(
                "Queueing score compute for queue_id=%s score_type=%s",
                pending_queue_item.queue_id,
                pending_queue_item.score_type,
            )
            pending_queue_item.compute_status = (
                ScoreComputeStatusEnum.PROCESSING.value
            )
            session.add(pending_queue_item)

            celery_app.send_task(
                Workers.SCORE_COMPUTE_WORKER,
                args=(pending_queue_item.queue_id,),
                queue=_config.worker_queue,
            )

    session.commit()
    _logger.info("Completed processing pending score compute requests")

