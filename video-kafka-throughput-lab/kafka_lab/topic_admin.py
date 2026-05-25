from __future__ import annotations

import argparse

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from kafka_lab.common import resolve_bootstrap


def create_topic(
    bootstrap_servers: str,
    topic: str,
    partitions: int,
    replicas: int,
    timeout_s: int = 30,
) -> bool:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    future = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=replicas)]
    )[topic]
    try:
        future.result(timeout=timeout_s)
        return True
    except KafkaException as exc:
        err = exc.args[0]
        if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            return False
        raise


def delete_topic(bootstrap_servers: str, topic: str, timeout_s: int = 30) -> bool:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    future = admin.delete_topics([topic])[topic]
    try:
        future.result(timeout=timeout_s)
        return True
    except KafkaException as exc:
        err = exc.args[0]
        if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            return False
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or delete Kafka topics.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("--topic", required=True)
    create_parser.add_argument("--partitions", type=int, required=True)
    create_parser.add_argument("--replicas", type=int, default=1)
    create_parser.add_argument("--bootstrap-servers", default=None)

    delete_parser = subparsers.add_parser("delete")
    delete_parser.add_argument("--topic", required=True)
    delete_parser.add_argument("--bootstrap-servers", default=None)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bootstrap = resolve_bootstrap(args.bootstrap_servers)

    if args.command == "create":
        created = create_topic(
            bootstrap_servers=bootstrap,
            topic=args.topic,
            partitions=args.partitions,
            replicas=args.replicas,
        )
        print(
            f"Topic {args.topic!r} {'created' if created else 'already exists'} "
            f"(partitions={args.partitions}, replicas={args.replicas})."
        )
        return

    deleted = delete_topic(bootstrap_servers=bootstrap, topic=args.topic)
    print(f"Topic {args.topic!r} {'deleted' if deleted else 'did not exist'}.")


if __name__ == "__main__":
    main()
