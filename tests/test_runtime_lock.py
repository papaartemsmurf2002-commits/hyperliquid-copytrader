from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import replace
from decimal import Decimal

import pytest

from hyperliquid_copytrader.config import ExchangeConfig
from hyperliquid_copytrader.exchange.hyperliquid import FakeExecutionAdapter, PreSendBlockedError
from hyperliquid_copytrader.models import Mode, SafeModeReason
from hyperliquid_copytrader.persistence import SQLiteStore
from hyperliquid_copytrader.runtime_lock import AccountRuntimeFileLock, RuntimeFileLockBusy
from hyperliquid_copytrader.service import CopyTraderService

from .fixtures.fake_hyperliquid import FakeInfoClient


FOLLOWER = "0xf000000000000000000000000000000000000000"


def test_account_global_lock_blocks_different_databases_after_db_ttl(base_config, tmp_path):
    lock_dir = tmp_path / "account-locks"
    ops = replace(base_config.ops, runtime_lease_ttl_ms=5, runtime_lock_dir=lock_dir)
    exchange = ExchangeConfig(
        follower_account_address=FOLLOWER,
        api_private_key="0x" + "1" * 64,
        testnet_enable=True,
    )
    config_one = replace(
        base_config,
        mode=Mode.TESTNET,
        db_path=tmp_path / "one.sqlite3",
        exchange=exchange,
        ops=ops,
    )
    config_two = replace(config_one, db_path=tmp_path / "two.sqlite3")
    store_one = SQLiteStore(config_one.db_path)
    store_two = SQLiteStore(config_two.db_path)
    service_one = CopyTraderService(
        config_one,
        store=store_one,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    service_two = CopyTraderService(
        config_two,
        store=store_two,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    try:
        assert service_one._acquire_exchange_lease("run_once")
        time.sleep(0.02)

        assert not service_two._acquire_exchange_lease("settle_pending")
        assert service_two.safe_mode.reason == SafeModeReason.CONCURRENT_INSTANCE

        service_one._release_exchange_lease("run_once")
        assert service_two._acquire_exchange_lease("settle_pending")
        service_two._release_exchange_lease("settle_pending")
        lock_files = list(lock_dir.glob("testnet-*.lock"))
        assert len([path for path in lock_files if "-signer-" not in path.name]) == 1
        assert len([path for path in lock_files if "-signer-" in path.name]) == 0
    finally:
        service_one._release_exchange_lease("run_once")
        service_two._release_exchange_lease("settle_pending")
        store_one.close()
        store_two.close()


def test_signer_global_lock_serializes_signed_actions_across_target_accounts(base_config, tmp_path):
    lock_dir = tmp_path / "signer-locks"
    ops = replace(
        base_config.ops,
        runtime_lock_dir=lock_dir,
        exchange_action_timeout_s=Decimal("0.01"),
    )
    private_key = "0x" + "2" * 64
    config_one = replace(
        base_config,
        mode=Mode.TESTNET,
        db_path=tmp_path / "target-one.sqlite3",
        exchange=ExchangeConfig(
            follower_account_address="0xf111111111111111111111111111111111111111",
            api_private_key=private_key,
            testnet_enable=True,
        ),
        ops=ops,
    )
    config_two = replace(
        config_one,
        db_path=tmp_path / "target-two.sqlite3",
        exchange=replace(
            config_one.exchange,
            follower_account_address="0xf222222222222222222222222222222222222222",
        ),
    )
    store_one = SQLiteStore(config_one.db_path)
    store_two = SQLiteStore(config_two.db_path)
    service_one = CopyTraderService(
        config_one,
        store=store_one,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    service_two = CopyTraderService(
        config_two,
        store=store_two,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    try:
        assert service_one._runtime_file_lock_path() != service_two._runtime_file_lock_path()
        assert (
            service_one._runtime_signer_file_lock_path()
            == service_two._runtime_signer_file_lock_path()
        )
        assert service_one._acquire_exchange_lease("run_once")
        assert service_two._acquire_exchange_lease("run_once")
        with service_one._signed_action_guard("first"):
            with pytest.raises(PreSendBlockedError, match="signer-global nonce lock"):
                with service_two._signed_action_guard("second"):
                    pass
        assert service_two.safe_mode.reason == SafeModeReason.CONCURRENT_INSTANCE

        with service_two._signed_action_guard("second-after-release"):
            pass
    finally:
        service_one._release_exchange_lease("run_once")
        service_two._release_exchange_lease("run_once")
        store_one.close()
        store_two.close()


def test_os_lock_is_released_when_owner_process_is_killed(tmp_path):
    lock_path = tmp_path / "crash-safe.lock"
    script = """
import sys
import time
from hyperliquid_copytrader.runtime_lock import AccountRuntimeFileLock

lock = AccountRuntimeFileLock(sys.argv[1])
lock.acquire()
print("locked", flush=True)
time.sleep(30)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(lock_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    contender = AccountRuntimeFileLock(lock_path)
    recovered = AccountRuntimeFileLock(lock_path)
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "locked"
        with pytest.raises(RuntimeFileLockBusy):
            contender.acquire()

        process.kill()
        process.wait(timeout=10)
        for attempt in range(20):
            try:
                recovered.acquire()
                break
            except RuntimeFileLockBusy:
                if attempt == 19:
                    raise
                time.sleep(0.05)
        assert recovered.acquired
    finally:
        recovered.release()
        contender.release()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
