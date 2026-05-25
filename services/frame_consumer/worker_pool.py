from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any

from confluent_kafka import Message, Producer

from services.frame_consumer.triton_client import (
    TritonSequenceClient,
    video_uuid_to_sequence_id,
)
from services.frame_consumer.video_renderer import VideoRenderManager
from shared.config import FrameRenderSettings, TritonSettings
from shared.image_utils import decode_base64_jpeg_to_frame
from shared.kafka_utils import json_loads, produce_json
from shared.schemas import (
    END_OF_STREAM_EVENT,
    FRAME_EVENT,
    validate_end_of_stream,
    validate_raw_frame,
)


logger = logging.getLogger("frame-consumer.worker")


# Bound queue depth so a slow worker eventually applies backpressure to the
# main poll loop instead of growing memory without limit.
DEFAULT_QUEUE_SIZE = 64

# Before asking the main thread to commit an input offset, flush the detections
# producer so the corresponding output is durable on the broker first.
PRODUCER_FLUSH_TIMEOUT_SECONDS = 30.0

# Emit a compact in-progress health line every N processed frames per video.
IN_PROGRESS_LOG_EVERY_FRAMES = 25


@dataclass(frozen=True)
class CommitRequest:
    topic: str
    partition: int
    next_offset: int
    video_uuid: str


@dataclass
class VideoProgress:
    video_name: str
    frames_processed: int = 0
    detections_emitted: int = 0
    last_sampled_frame_id: int = -1


def _metadata_for_triton(
    message: dict[str, Any],
    *,
    sequence_id: int,
    sequence_start: bool,
    sequence_end: bool,
) -> dict[str, Any]:
    metadata = {key: value for key, value in message.items() if key != "image_base64"}
    metadata["sequence_id"] = sequence_id
    metadata["sequence_start"] = sequence_start
    metadata["sequence_end"] = sequence_end
    return metadata


class Worker:
    """Single-partition worker. Runs in its own thread, processes Kafka offsets
    in partition order, and emits a commit request after each processed record.

    Each worker owns its own ``TritonSequenceClient`` because the underlying
    ``tritonclient.http`` implementation is built on gevent greenlets, which
    are bound to a single OS thread and cannot be shared across threads."""

    def __init__(
        self,
        *,
        partition: int,
        triton_settings: TritonSettings,
        frame_render_settings: FrameRenderSettings,
        producer: Producer,
        detections_topic: str,
        commit_queue: "Queue[CommitRequest]",
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        self.partition = partition
        self._triton_settings = triton_settings
        self._producer = producer
        self._detections_topic = detections_topic
        self._commit_queue = commit_queue
        self._renderer = VideoRenderManager(frame_render_settings)
        self._sequence_started_by_video_uuid: dict[str, bool] = {}
        self._progress_by_video_uuid: dict[str, VideoProgress] = {}
        self._queue: "Queue[Message | None]" = Queue(maxsize=queue_size)
        self._shutdown_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"frame-consumer-partition-{partition}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def enqueue(self, msg: Message) -> None:
        # Blocking put: provides natural backpressure if Triton can't keep up.
        self._queue.put(msg)

    def request_shutdown(self) -> None:
        self._shutdown_event.set()
        # Wake up the worker if it's blocked on Queue.get().
        try:
            self._queue.put_nowait(None)
        except Exception:  # noqa: BLE001
            # Queue might be full; the event flag will be observed on next iteration.
            pass

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        # Build the Triton client inside the worker thread so its underlying
        # gevent hub is created on this thread.
        triton = TritonSequenceClient(
            self._triton_settings.url,
            self._triton_settings.model_name,
            self._triton_settings.request_timeout_seconds,
        )
        logger.info("Partition worker started partition=%s", self.partition)
        try:
            while True:
                if self._shutdown_event.is_set():
                    logger.info("Worker shutdown signal received partition=%s", self.partition)
                    return
                try:
                    msg = self._queue.get(timeout=1.0)
                except Empty:
                    continue
                if msg is None:
                    logger.info("Worker shutdown signal received partition=%s", self.partition)
                    return
                if not self._handle_message(triton, msg):
                    logger.error(
                        "Stopping partition worker after failed record partition=%s offset=%s",
                        msg.partition(),
                        msg.offset(),
                    )
                    return
        except Exception:  # noqa: BLE001
            logger.exception("Worker crashed partition=%s", self.partition)
        finally:
            self._renderer.finalize_all()
            self._finalize_incomplete_videos()

    def _handle_message(self, triton: TritonSequenceClient, msg: Message) -> bool:
        """Process one Kafka record and request a commit only after its side
        effects have been flushed. Returns False when the partition should stop
        so later offsets cannot be committed past a failed record."""
        try:
            message = json_loads(msg.value())
            video_uuid = str(message.get("video_uuid", ""))
            if not video_uuid:
                raise ValueError("Message is missing video_uuid")
            key = msg.key().decode("utf-8") if msg.key() else ""
            if key and key != video_uuid:
                raise ValueError(f"Kafka key {key!r} does not match video_uuid {video_uuid!r}")

            event_type = message.get("event_type")
            if event_type == FRAME_EVENT:
                self._process_frame(triton, message)
                self._flush_and_request_commit(msg, video_uuid)
                return True
            if event_type == END_OF_STREAM_EVENT:
                self._process_end_of_stream(triton, message)
                self._flush_and_request_commit(msg, video_uuid)
                return True
            logger.warning(
                "Worker skipping unknown event_type=%r video_uuid=%s partition=%s offset=%s",
                event_type,
                video_uuid,
                msg.partition(),
                msg.offset(),
            )
            self._flush_and_request_commit(msg, video_uuid)
            return True
        except Exception:  # noqa: BLE001
            logger.exception(
                "Worker failed to process message partition=%s offset=%s",
                msg.partition(),
                msg.offset(),
            )
            return False

    def _process_frame(self, triton: TritonSequenceClient, message: dict[str, Any]) -> None:
        validate_raw_frame(message)
        video_uuid = str(message["video_uuid"])
        progress = self._get_or_create_progress(message)
        sequence_id = video_uuid_to_sequence_id(video_uuid)
        frame = decode_base64_jpeg_to_frame(str(message["image_base64"]))
        sequence_start = not self._sequence_started_by_video_uuid.get(video_uuid, False)
        output = triton.infer(
            frame=frame,
            metadata=_metadata_for_triton(
                message,
                sequence_id=sequence_id,
                sequence_start=sequence_start,
                sequence_end=False,
            ),
            sequence_id=sequence_id,
            sequence_start=sequence_start,
            sequence_end=False,
        )
        self._sequence_started_by_video_uuid[video_uuid] = True
        produce_json(self._producer, self._detections_topic, video_uuid, output)
        self._renderer.render_frame(
            message=message,
            frame=frame,
            detections=list(output.get("detections", [])),
        )
        sampled_frame_id = int(output.get("sampled_frame_id", message["sampled_frame_id"]))
        detections_count = len(output.get("detections", []))
        progress.frames_processed += 1
        progress.detections_emitted += detections_count
        progress.last_sampled_frame_id = sampled_frame_id
        if (
            progress.frames_processed == 1
            or progress.frames_processed % IN_PROGRESS_LOG_EVERY_FRAMES == 0
        ):
            logger.info(
                "VIDEO_STATUS status=in_progress partition=%s video_uuid=%s video_name=%s frames_processed=%s detections_emitted=%s last_sampled_frame_id=%s",
                self.partition,
                video_uuid,
                progress.video_name,
                progress.frames_processed,
                progress.detections_emitted,
                progress.last_sampled_frame_id,
            )
        logger.info(
            "Worker partition=%s video_uuid=%s published detections sampled_frame_id=%s detections=%s",
            self.partition,
            video_uuid,
            sampled_frame_id,
            detections_count,
        )

    def _process_end_of_stream(self, triton: TritonSequenceClient, message: dict[str, Any]) -> None:
        validate_end_of_stream(message)
        video_uuid = str(message["video_uuid"])
        sequence_id = video_uuid_to_sequence_id(video_uuid)
        sequence_start = not self._sequence_started_by_video_uuid.get(video_uuid, False)
        triton.infer(
            frame=None,
            metadata=_metadata_for_triton(
                message,
                sequence_id=sequence_id,
                sequence_start=sequence_start,
                sequence_end=True,
            ),
            sequence_id=sequence_id,
            sequence_start=sequence_start,
            sequence_end=True,
            expect_output=False,
        )
        self._sequence_started_by_video_uuid.pop(video_uuid, None)
        self._renderer.mark_end_of_stream(message)
        progress = self._progress_by_video_uuid.pop(video_uuid, None)
        last_sampled_frame_id = int(message.get("last_sampled_frame_id", -1))
        frames_processed = progress.frames_processed if progress is not None else 0
        detections_emitted = progress.detections_emitted if progress is not None else 0
        video_name = str(message.get("video_name", progress.video_name if progress else "unknown"))
        logger.info(
            "VIDEO_STATUS status=finalized partition=%s video_uuid=%s video_name=%s reason=eos frames_processed=%s detections_emitted=%s last_sampled_frame_id=%s",
            self.partition,
            video_uuid,
            video_name,
            frames_processed,
            detections_emitted,
            last_sampled_frame_id,
        )
        logger.info(
            "Worker partition=%s video_uuid=%s ended Triton sequence_id=%s",
            self.partition,
            video_uuid,
            sequence_id,
        )

    def _get_or_create_progress(self, message: dict[str, Any]) -> VideoProgress:
        video_uuid = str(message["video_uuid"])
        progress = self._progress_by_video_uuid.get(video_uuid)
        if progress is not None:
            return progress
        video_name = str(message.get("video_name", "unknown"))
        progress = VideoProgress(video_name=video_name)
        self._progress_by_video_uuid[video_uuid] = progress
        logger.info(
            "VIDEO_STATUS status=started partition=%s video_uuid=%s video_name=%s",
            self.partition,
            video_uuid,
            video_name,
        )
        return progress

    def _finalize_incomplete_videos(self) -> None:
        for video_uuid, progress in list(self._progress_by_video_uuid.items()):
            logger.warning(
                "VIDEO_STATUS status=finalized partition=%s video_uuid=%s video_name=%s reason=shutdown_before_eos frames_processed=%s detections_emitted=%s last_sampled_frame_id=%s",
                self.partition,
                video_uuid,
                progress.video_name,
                progress.frames_processed,
                progress.detections_emitted,
                progress.last_sampled_frame_id,
            )
        self._progress_by_video_uuid.clear()

    def _flush_and_request_commit(self, msg: Message, video_uuid: str) -> None:
        remaining = self._producer.flush(PRODUCER_FLUSH_TIMEOUT_SECONDS)
        if remaining:
            raise TimeoutError(f"Timed out flushing {remaining} detection message(s)")
        request = CommitRequest(
            topic=msg.topic(),
            partition=msg.partition(),
            next_offset=msg.offset() + 1,
            video_uuid=video_uuid,
        )
        self._commit_queue.put(request)
        logger.info(
            "Worker enqueued commit request partition=%s next_offset=%s video_uuid=%s",
            request.partition,
            request.next_offset,
            video_uuid,
        )


class WorkerPool:
    """Lazy pool of one Worker per Kafka partition."""

    def __init__(
        self,
        *,
        triton_settings: TritonSettings,
        frame_render_settings: FrameRenderSettings,
        producer: Producer,
        detections_topic: str,
        commit_queue: "Queue[CommitRequest]",
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        self._triton_settings = triton_settings
        self._frame_render_settings = frame_render_settings
        self._producer = producer
        self._detections_topic = detections_topic
        self._commit_queue = commit_queue
        self._queue_size = queue_size
        self._workers: dict[int, Worker] = {}
        self._lock = threading.Lock()

    def dispatch(self, msg: Message) -> None:
        worker = self._get_or_create(msg.partition())
        if not worker.is_alive:
            raise RuntimeError(f"Partition worker is not running: partition={msg.partition()}")
        worker.enqueue(msg)

    def remove(self, partition: int) -> None:
        with self._lock:
            worker = self._workers.pop(partition, None)
        if worker is not None:
            worker.join(timeout=5.0)
            if worker.is_alive:
                logger.warning("Worker did not exit cleanly partition=%s", partition)

    def shutdown_all(self) -> None:
        with self._lock:
            workers = list(self._workers.values())
            self._workers.clear()
        for worker in workers:
            worker.request_shutdown()
        for worker in workers:
            worker.join(timeout=5.0)

    def drain_commit_queue(self) -> list[CommitRequest]:
        requests: list[CommitRequest] = []
        while True:
            try:
                requests.append(self._commit_queue.get_nowait())
            except Empty:
                break
        return requests

    def _get_or_create(self, partition: int) -> Worker:
        with self._lock:
            worker = self._workers.get(partition)
            if worker is None:
                worker = Worker(
                    partition=partition,
                    triton_settings=self._triton_settings,
                    frame_render_settings=self._frame_render_settings,
                    producer=self._producer,
                    detections_topic=self._detections_topic,
                    commit_queue=self._commit_queue,
                    queue_size=self._queue_size,
                )
                self._workers[partition] = worker
                worker.start()
            return worker
