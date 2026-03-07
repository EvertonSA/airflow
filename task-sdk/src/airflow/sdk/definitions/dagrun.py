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
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.sdk.api.datamodels._generated import DagRun as DagRunResponse


class DagRun:
    """
    Client-side DagRun abstraction for Task SDK.
    """

    @staticmethod
    def find(
        *,
        dag_id: str | list[str] | None = None,
        run_id: Iterable[str] | None = None,
        execution_date: datetime | Iterable[datetime] | None = None,
        state: str | Iterable[str] | None = None,
        external_trigger: bool | None = None,
        no_backfills: bool = False,
        limit: int = 30,
        execution_start_date: datetime | None = None,
        execution_end_date: datetime | None = None,
    ) -> list[DagRunResponse]:
        """
        Return a list of dag runs for a given dag_id.

        This method sends a request to the API server via the supervisor to fetch the list of DagRuns.
        """
        from airflow.sdk.execution_time.comms import DagRunListResult, GetDagRuns
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        
        # Ensure we send appropriate types over IPC
        if isinstance(dag_id, str):
            dag_id = [dag_id]
        if isinstance(run_id, str):
            run_id = [run_id]
        if execution_date is not None and not isinstance(execution_date, Iterable):
            execution_date = [execution_date]
        if isinstance(state, str):
            state = [state]
        
        msg = GetDagRuns(
            dag_ids=list(dag_id) if dag_id else None,
            run_ids=list(run_id) if run_id else None,
            logical_dates=list(execution_date) if execution_date else None,
            states=list(state) if state else None,
            external_trigger=external_trigger,
            no_backfills=no_backfills,
            limit=limit,
            logical_start_date=execution_start_date,
            logical_end_date=execution_end_date,
        )
        resp = SUPERVISOR_COMMS.send(msg)

        from airflow.sdk.exceptions import AirflowRuntimeError
        from airflow.sdk.execution_time.comms import ErrorResponse
        
        if isinstance(resp, ErrorResponse):
            raise AirflowRuntimeError(resp)

        if TYPE_CHECKING:
            assert isinstance(resp, DagRunListResult)

        return list(resp.dag_runs)
