import sys

from kie.health import HealthSupervisor
from kie.transports.engine_event_socket import EngineEventSocketServer


class _PluginContext:
    def has_plugin(self, plugin_id: str) -> bool:
        return True


def test_second_server_observes_existing_owner_without_taking_or_removing_socket(tmp_path):
    socket_path = tmp_path / "run" / "kie-events.sock"
    database = tmp_path / "kie.sqlite3"
    owner = EngineEventSocketServer(socket_path, database)
    observer = EngineEventSocketServer(socket_path, database)

    owner.start()
    try:
        observer.start()
        assert owner.status == "owner"
        assert observer.status == "connected"

        observer.shutdown()
        assert socket_path.is_socket()
    finally:
        owner.shutdown()

    assert not socket_path.exists()


def test_health_probe_does_not_create_an_event_or_initialize_the_database(tmp_path):
    socket_path = tmp_path / "run" / "kie-events.sock"
    database = tmp_path / "kie.sqlite3"
    owner = EngineEventSocketServer(socket_path, database)
    observer = EngineEventSocketServer(socket_path, database)

    owner.start()
    try:
        observer.start()
        assert observer.status == "connected"
        assert not database.exists()
    finally:
        observer.shutdown()
        owner.shutdown()


def test_observer_can_take_ownership_after_previous_owner_releases_lock(tmp_path):
    socket_path = tmp_path / "run" / "kie-events.sock"
    database = tmp_path / "kie.sqlite3"
    owner = EngineEventSocketServer(socket_path, database)
    successor = EngineEventSocketServer(socket_path, database)

    owner.start()
    successor.start()
    assert successor.status == "connected"

    owner.shutdown()
    successor.start()

    try:
        assert successor.status == "owner"
        assert socket_path.is_socket()
    finally:
        successor.shutdown()


def test_health_report_distinguishes_owner_from_connected_observer(tmp_path):
    socket_path = tmp_path / "run" / "kie-events.sock"
    database = tmp_path / "kie.sqlite3"
    owner = EngineEventSocketServer(socket_path, database)
    observer = EngineEventSocketServer(socket_path, database)
    owner.start()

    try:
        observer.start()
        supervisor = HealthSupervisor(
            _PluginContext(),
            kie_database=tmp_path / "missing-kie.sqlite3",
            event_server=observer,
            evidence_python=sys.executable,
            evidence_source_root=tmp_path / "missing-evidence",
            evidence_database=tmp_path / "missing-evidence.sqlite3",
        )
        report = supervisor.get_report()

        assert report["event_ingress"] == "ready"
        assert report["event_ingress_role"] == "connected"
    finally:
        observer.shutdown()
        owner.shutdown()


def test_health_report_marks_local_receiver_as_owner(tmp_path):
    socket_path = tmp_path / "run" / "kie-events.sock"
    database = tmp_path / "kie.sqlite3"
    owner = EngineEventSocketServer(socket_path, database)
    owner.start()

    try:
        supervisor = HealthSupervisor(
            _PluginContext(),
            kie_database=tmp_path / "missing-kie.sqlite3",
            event_server=owner,
            evidence_python=sys.executable,
            evidence_source_root=tmp_path / "missing-evidence",
            evidence_database=tmp_path / "missing-evidence.sqlite3",
        )
        report = supervisor.get_report()

        assert report["event_ingress"] == "ready"
        assert report["event_ingress_role"] == "owner"
    finally:
        owner.shutdown()
