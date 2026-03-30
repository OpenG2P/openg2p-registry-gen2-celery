import logging
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from openg2p_registry_core.models import G2PFunctionalIdGenerationQueue
from openg2p_registry_core.models.g2p_functional_id_generation_queue import (
    ProcessStatusEnum as FunctionalIdGenerationStatusEnum,
)

from ..app import celery_app
from ..config import Settings
from ..engine import Engine
from ..utils import Workers

_config = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)
_engine = Engine.get_engine()


@celery_app.task(name="functional_id_updation_beat_producer")
def functional_id_updation_beat_producer():
    _logger.info("Checking for pending functional ID updation requests")
    session_maker = sessionmaker(bind=_engine, expire_on_commit=False)

    with session_maker() as session:
        pending_queue_items: List[G2PFunctionalIdGenerationQueue] = (
            session.execute(
                select(G2PFunctionalIdGenerationQueue)
                .filter(
                    G2PFunctionalIdGenerationQueue.id_updation_status
                    == FunctionalIdGenerationStatusEnum.PENDING.value
                )
                .limit(_config.no_of_tasks_to_process)
            )
            .scalars()
            .all()
        )
        _logger.info(
            f"Found {len(pending_queue_items)} PENDING functional ID updation requests"
        )

        for queue_item in pending_queue_items:
            _logger.info(
                f"Queueing functional ID updation for queue_id: {queue_item.queue_id}"
            )

            queue_item.id_updation_status = FunctionalIdGenerationStatusEnum.PROCESSING.value
            session.add(queue_item)

            celery_app.send_task(
                Workers.FUNCTIONAL_ID_UPDATION_WORKER,
                args=(queue_item.queue_id,),
                queue=_config.worker_queue,
            )
            _logger.info(
                f"Sent task to {Workers.FUNCTIONAL_ID_UPDATION_WORKER} for queue_id: {queue_item.queue_id}"
            )

        session.commit()

    _logger.info("Completed processing pending functional ID updation requests")
