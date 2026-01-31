from __future__ import annotations

import os
import shutil
import time


def maybe_reset_checkpoint(
    checkpoint_dir: str,
    *,
    enabled: bool,
) -> None:
    """체크포인트 초기화 여부를 처리한다."""
    exists = os.path.exists(checkpoint_dir)
    if not enabled:
        if exists:
            print(
                "[ℹ️ checkpoint] reset 비활성 "
                f"(SPARK_RESET_CHECKPOINT_ON_START=false), 기존 사용: {checkpoint_dir}"
            )
        return
    if not exists:
        return

    ts = time.strftime("%Y%m%d-%H%M%S")
    backup = f"{checkpoint_dir}.bak.{ts}"
    print(f"[⚠️ checkpoint] reset 활성: 이동 {checkpoint_dir} -> {backup}")
    shutil.move(checkpoint_dir, backup)
    os.makedirs(checkpoint_dir, exist_ok=True)
