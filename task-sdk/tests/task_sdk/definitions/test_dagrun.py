# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import typing
from datetime import datetime, timezone

import pytest

from airflow.sdk.api.datamodels._generated import DagRun as DagRunResponse
from airflow.sdk.definitions.dagrun import DagRun
from airflow.sdk.execution_time.comms import DagRunListResult, ErrorResponse, GetDagRuns
from airflow.sdk.exceptions import AirflowRuntimeError


class TestDagRun:
    def test_find_sends_correct_msg(self, monkeypatch):
        sent_messages = []

        class MockComms:
            def send(self, msg):
                sent_messages.append(msg)
                return DagRunListResult(
                    dag_runs=[
                        DagRunResponse(
                            id="some-id",
                            dag_id="test_dag",
                            run_id="test_run",
                            state="success",
                            run_type="manual",
                            logical_date=None,
                            data_interval_start=None,
                            data_interval_end=None,
                            run_after=datetime.now(timezone.utc),
                            start_date=None,
                            end_date=None,
                            consumed_asset_events=[],
                            partition_key=None,
                        )
                    ]
                )

        import airflow.sdk.definitions.dagrun
        monkeypatch.setattr(airflow.sdk.definitions.dagrun, "SUPERVISOR_COMMS", MockComms())

        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        runs = DagRun.find(
            dag_id="test_dag",
            run_id="test_run",
            execution_date=dt,
            state="success",
            external_trigger=True,
            no_backfills=True,
            limit=50,
        )

        assert len(sent_messages) == 1
        msg = sent_messages[0]
        assert isinstance(msg, GetDagRuns)
        assert msg.dag_ids == ["test_dag"]
        assert msg.run_ids == ["test_run"]
        assert msg.logical_dates == [dt]
        assert msg.states == ["success"]
        assert msg.external_trigger is True
        assert msg.no_backfills is True
        assert msg.limit == 50
        
        assert len(runs) == 1
        assert runs[0].dag_id == "test_dag"

    def test_find_handles_error_response(self, monkeypatch):
        class MockComms:
            def send(self, msg):
                return ErrorResponse(error="some_error", detail={"reason": "testing"})

        import airflow.sdk.definitions.dagrun
        monkeypatch.setattr(airflow.sdk.definitions.dagrun, "SUPERVISOR_COMMS", MockComms())

        with pytest.raises(AirflowRuntimeError):
            DagRun.find(dag_id="test_dag")
