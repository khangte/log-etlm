from __future__ import annotations

import logging
import os
import shutil
import time

logger = logging.getLogger(__name__)


def maybe_reset_checkpoint(
    checkpoint_dir: str,
    *,
    enabled: bool,
) -> None:
    """체크포인트 초기화 여부를 처리한다."""
    exists = os.path.exists(checkpoint_dir)
    if not enabled:
        if exists:
            logger.info(
                "[INFO] checkpoint reset 비활성 "
                "(SPARK_RESET_CHECKPOINT_ON_START=false), 기존 사용: %s",
                checkpoint_dir,
            )
        return
    if not exists:
        return

    ts = time.strftime("%Y%m%d-%H%M%S")
    backup = f"{checkpoint_dir}.bak.{ts}"
    logger.info("[INFO] checkpoint reset 활성: 이동 %s -> %s", checkpoint_dir, backup)
    shutil.move(checkpoint_dir, backup)
    os.makedirs(checkpoint_dir, exist_ok=True)
