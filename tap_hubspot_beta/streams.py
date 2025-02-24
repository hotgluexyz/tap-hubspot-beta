"""Stream type classes for tap-hubspot."""
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
import copy

from singer_sdk.exceptions import InvalidStreamSortException
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.exceptions import FatalAPIError
import singer
import logging

import requests
from backports.cached_property import cached_property
from singer_sdk import typing as th
from pendulum import parse

from tap_hubspot_beta.client_base import hubspotStreamSchema
from tap_hubspot_beta.client_v1 import hubspotV1Stream
from tap_hubspot_beta.client_v3 import hubspotV3SearchStream, hubspotV3Stream, hubspotV3SingleSearchStream, hubspotHistoryV3Stream
from tap_hubspot_beta.client_v4 import hubspotV4Stream
from tap_hubspot_beta.client_v2 import hubspotV2Stream
import time
import pytz
from pendulum import parse
from urllib.parse import urlencode
import json
from itertools import groupby

from singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error
)

association_schema = th.PropertiesList(
        th.Property("from_id", th.StringType),
        th.Property("to_id", th.StringType),
        th.Property("typeId", th.NumberType),
        th.Property("category", th.StringType),
        th.Property("label", th.StringType),
        th.Property("associationTypes", th.CustomType({"type": ["array", "object"]})),
    ).to_dict()

class AccountStream(hubspotV1Stream):
    """Account Stream"""

    name = "account"
    path = "integrations/v1/me"
    records_jsonpath = "$"
    primary_keys = ["portalId"]

    schema = th.PropertiesList(
        th.Property("portalId", th.IntegerType),
        th.Property("timeZone", th.StringType),
        th.Property("accountType", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("utcOffset", th.StringType),
        th.Property("utcOffsetMilliseconds", th.IntegerType),
    ).to_dict()


class DispositionsStream(hubspotV1Stream):
    """Dispositions Stream"""

    name = "dispositions"
    path = "calling/v1/dispositions"
    records_jsonpath = "$.[*]"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("label", th.StringType),
        th.Property("deleted", th.BooleanType)
    ).to_dict()


class EngagementStream(hubspotV1Stream):
    """Engagement Stream"""

    name = "engagements"
    path = "engagements/v1/engagements/paged"
    records_jsonpath = "$.results[*]"
    primary_keys = ["id"]
    replication_key = None
    page_size = 250
    properties_url = "properties/v2/engagements/properties"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("portalId", th.IntegerType),
        th.Property("active", th.BooleanType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("lastUpdated", th.DateTimeType),
        th.Property("createdBy", th.IntegerType),
        th.Property("modifiedBy", th.IntegerType),
        th.Property("ownerId", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("uid", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("source", th.StringType),
        th.Property("allAccessibleTeamIds", th.ArrayType(th.IntegerType)),
        th.Property("queueMembershipIds", th.ArrayType(th.IntegerType)),
        th.Property("bodyPreview", th.StringType),
        th.Property("bodyPreviewIsTruncated", th.BooleanType),
        th.Property("bodyPreviewHtml", th.StringType),
        th.Property("gdprDeleted", th.BooleanType),
        th.Property("contactIds", th.ArrayType(th.IntegerType)),
        th.Property("companyIds", th.ArrayType(th.IntegerType)),
        th.Property("dealIds", th.ArrayType(th.IntegerType)),
        th.Property("ownerIds", th.ArrayType(th.IntegerType)),
        th.Property("workflowIds", th.ArrayType(th.IntegerType)),
        th.Property("ticketIds", th.ArrayType(th.IntegerType)),
        th.Property("contentIds", th.ArrayType(th.IntegerType)),
        th.Property("quoteIds", th.ArrayType(th.IntegerType)),
        th.Property("status", th.StringType),
        th.Property("forObjectType", th.StringType),
        th.Property("subject", th.StringType),
        th.Property("taskType", th.StringType),
        th.Property("reminders", th.ArrayType(th.IntegerType)),
        th.Property("sendDefaultReminder", th.BooleanType),
        th.Property("priority", th.StringType),
        th.Property("isAllDay", th.BooleanType),
        th.Property("body", th.StringType),
        th.Property("disposition", th.StringType),
        th.Property("toNumber", th.StringType),
        th.Property("fromNumber", th.StringType),
        th.Property("durationMilliseconds", th.IntegerType),
        th.Property("recordingUrl", th.StringType),
        th.Property("title", th.StringType),
        th.Property("completionDate", th.DateTimeType),
        th.Property("from", th.CustomType({"type": ["object", "string"]})),
        th.Property("to", th.CustomType({"type": ["array", "string"]})),
        th.Property("cc", th.CustomType({"type": ["array", "string"]})),
        th.Property("bcc", th.CustomType({"type": ["array", "string"]})),
        th.Property("sender", th.CustomType({"type": ["object", "string"]})),
        th.Property("text", th.StringType),
        th.Property("html", th.StringType),
        th.Property("trackerKey", th.StringType),
        th.Property("messageId", th.StringType),
        th.Property("threadId", th.StringType),
        th.Property("emailSendEventId", th.CustomType({"type": ["object", "string"]})),
        th.Property("loggedFrom", th.StringType),
        th.Property("validationSkipped", th.CustomType({"type": ["array", "string"]})),
        th.Property("postSendStatus", th.StringType),
        th.Property("mediaProcessingStatus", th.StringType),
        th.Property("attachedVideoOpened", th.BooleanType),
        th.Property("attachedVideoWatched", th.BooleanType),
        th.Property("pendingInlineImageIds", th.CustomType({"type": ["array", "string"]}))
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        flaten_row = {}
        for group in ["engagement", "associations", "metadata"]:
            flaten_row.update(row[group])
        row = super().post_process(flaten_row, context)
        # force threadId to be a string and keep one typing
        if row.get("threadId"):
            row["threadId"] = str(row.get("threadId"))
        return row


class ContactsStream(hubspotV1Stream):
    """Contacts Stream"""

    name = "contacts"
    path = "contacts/v1/lists/all/contacts/all"
    records_jsonpath = "$.contacts[*]"
    primary_keys = ["vid"]
    replication_key = None
    additional_prarams = dict(showListMemberships=True)
    properties_url = "properties/v1/contacts/properties"

    base_properties = [
        th.Property("vid", th.IntegerType),
        th.Property("addedAt", th.DateTimeType),
        th.Property("portal-id", th.IntegerType),
        th.Property("list-memberships", th.CustomType({"type": ["array", "string"]})),
        th.Property("subscriber_email", th.StringType)
    ]

    def parse_response(self, response):
        response_content = response.json()
        for record in extract_jsonpath(self.records_jsonpath, response_content):
            for identity_profile in record['identity-profiles']:
                    for identity in identity_profile["identities"]:
                        if identity['type'] == 'EMAIL':
                           record['subscriber_email'] = identity['value']
            yield record

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "contact_id": record["vid"],
            "contact_date": record.get("lastmodifieddate"),
            "subscriber_email": record.get("subscriber_email")
        }

    def get_child_bookmark(self, child_stream, child_context):
        state_date = None
        if child_stream.tap_state.get("bookmarks"):
            if child_stream.tap_state["bookmarks"].get(child_stream.name):
                child_state = child_stream.tap_state["bookmarks"][child_stream.name]
                if child_state.get("partitions"):
                    for partition in child_state["partitions"]:
                        if partition.get("context"):
                            key = list(child_context.keys())[0]
                            if partition["context"].get(key) == child_context[key]:
                                if partition.get("replication_key_value"):
                                    return parse(partition["replication_key_value"])
            return None
        if state_date:
            return parse(state_date)
        return state_date

    def _sync_children(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                last_job = self.last_job
                if child_stream.get_replication_key_signpost(child_context):
                    current_job = child_stream.get_replication_key_signpost(child_context)
                else:
                    current_job = datetime.utcnow()
                child_state = self.get_child_bookmark(child_stream, {"contact_id": child_context.get("contact_id")})
                full_event_sync = self.config.get("full_event_sync")
                partial_event_sync_lookup = self.config.get("partial_event_sync_lookup")

                # Test conditions to sync or not the events
                if not last_job or not full_event_sync:
                    child_stream.sync_custom(context=child_context)
                    self.tap_state["bookmarks"]["last_job"] = dict(value=current_job.isoformat())
                elif (last_job and full_event_sync and ((current_job-last_job).total_hours() >= full_event_sync)) and current_job.weekday()>=5:
                    self.tap_state["bookmarks"]["last_job"] = dict(value=current_job.isoformat())
                    child_stream.sync_custom(context=child_context)
                elif child_state and partial_event_sync_lookup:
                    if child_context.get("contact_date"):
                        updated_date = parse(child_context.get("contact_date"))
                        child_state = max(updated_date, child_state)
                    if (current_job-child_state).total_hours() < partial_event_sync_lookup:
                        child_stream.sync_custom(context=child_context)
                elif not child_state:
                    if child_context.get("contact_date"):
                        context_date = parse(child_context.get("contact_date"))
                        if (current_job-context_date).total_hours() < partial_event_sync_lookup:
                            child_stream.sync_custom(context=child_context)

                # set replication date to the contact create date
                if child_stream.tap_state.get("bookmarks"):
                    if child_stream.tap_state["bookmarks"].get(child_stream.name):
                        child_state = child_stream.tap_state["bookmarks"][child_stream.name]
                        if child_state.get("partitions"):
                            child_part = next((p for p in child_state["partitions"] if p["context"].get("contact_id")==child_context.get("contact_id")), None)
                            if child_part and ("replication_key" not in child_part):
                                child_part["replication_key"] = child_stream.replication_key
                                child_part["replication_key_value"] = child_context["contact_date"]


class ContactSubscriptionStatusStream(hubspotV3Stream):
    name = 'contact_subscription_status'
    path = 'communication-preferences/v3/status/email/{subscriber_email}'
    records_jsonpath = "$.[*]"
    parent_stream_type = ContactsStream
    ignore_parent_replication_keys = True
    schema_writed = False

    schema = th.PropertiesList(
        th.Property("recipient", th.StringType),
        th.Property("subscriptionStatuses", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("description", th.StringType),
                th.Property("status", th.StringType),
                th.Property("sourceOfStatus", th.StringType),
                th.Property("preferenceGroupName", th.StringType),
                th.Property("legalBasis", th.StringType),
                th.Property("legalBasisExplanation", th.StringType),
            )
        ))
    ).to_dict()

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    self._sync_children(child_context)
                self._check_max_record_limit(record_count)
                if selected:
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                self.finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            self.finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)

    schema_writed = False

    def sync_custom(self, context: Optional[dict] = None) -> None:
        msg = f"Beginning {self.replication_method.lower()} sync of '{self.name}'"
        if context:
            msg += f" with context: {context}"
        self.logger.info(f"{msg}...")
        # Use a replication signpost, if available
        signpost = self.get_replication_key_signpost(context)
        if signpost:
            self._write_replication_key_signpost(context, signpost)
        # Send a SCHEMA message to the downstream target:
        if not self.schema_writed:
            self._write_schema_message()
            self.schema_writed = True
        # Sync the records themselves:
        self._sync_records(context)


class ContactEventsStream(hubspotV3Stream):
    """ContactEvents Stream"""

    name = "contact_events"
    path = "events/v3/events/?objectType=contact&objectId={contact_id}"

    records_jsonpath = "$.results[*]"
    parent_stream_type = ContactsStream
    primary_keys = ["id"]
    replication_key = "occurredAt"

    schema = th.PropertiesList(
        th.Property("objectType", th.StringType),
        th.Property("objectId", th.StringType),
        th.Property("eventType", th.StringType),
        th.Property("occurredAt", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("contact_id", th.IntegerType),
        th.Property("properties", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = super().post_process(row, context)
        row["contact_id"] = context.get("contact_id")
        return row

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)

        child_part = {}
        if self.tap_state.get("bookmarks"):
            if self.tap_state["bookmarks"].get(self.name):
                child_state = self.tap_state["bookmarks"][self.name]
                if child_state.get("partitions"):
                    child_part = next((p for p in child_state["partitions"] if p["context"].get("contact_id")==context.get("contact_id")), None)
        if child_part.get("replication_key_value"):
            start_date = parse(child_part.get("replication_key_value"))
            params["occurredAfter"] = start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return params

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    self._sync_children(child_context)
                self._check_max_record_limit(record_count)
                if selected:
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                self.finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            self.finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)

    schema_writed = False

    def sync_custom(self, context: Optional[dict] = None) -> None:
        msg = f"Beginning {self.replication_method.lower()} sync of '{self.name}'"
        if context:
            msg += f" with context: {context}"
        self.logger.info(f"{msg}...")
        # Use a replication signpost, if available
        signpost = self.get_replication_key_signpost(context)
        if signpost:
            self._write_replication_key_signpost(context, signpost)
        # Send a SCHEMA message to the downstream target:
        if not self.schema_writed:
            self._write_schema_message()
            self.schema_writed = True
        # Sync the records themselves:
        self._sync_records(context)


class EmailEventsStream(hubspotV1Stream):
    """EmailEvents Stream"""

    name = "email_events"
    path = "email/public/v1/events"
    records_jsonpath = "$.events[*]"
    primary_keys = ["listId", "created"]
    replication_key = "created"
    page_size = 250

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("emailCampaignId", th.IntegerType),
        th.Property("hmid", th.StringType),
        th.Property("recipient", th.StringType),
        th.Property("type", th.StringType),
        th.Property("userAgent", th.StringType),
        th.Property("location", th.CustomType({"type": ["object", "string"]})),
        th.Property("browser", th.CustomType({"type": ["object", "string"]})),
        th.Property("portalId", th.IntegerType),
        th.Property("created", th.DateTimeType),
        th.Property("appName", th.StringType),
        th.Property("deviceType", th.StringType),
        th.Property("duration", th.IntegerType),
        th.Property("sentBy", th.CustomType({"type": ["object", "string"]})),
        th.Property("smtpId", th.StringType),
        th.Property("filteredEvent", th.BooleanType),
        th.Property("appId", th.IntegerType),
        th.Property("response", th.StringType),
        th.Property("attempt", th.IntegerType),
        th.Property("subject", th.StringType),
        th.Property("cc", th.CustomType({"type": ["array", "string"]})),
        th.Property("bcc", th.CustomType({"type": ["array", "string"]})),
        th.Property("replyTo", th.CustomType({"type": ["array", "string"]})),
        th.Property("from", th.StringType),
        th.Property("sourceId", th.StringType),
        th.Property("subscriptions", th.CustomType({"type": ["array", "string"]})),
        th.Property("portalSubscriptionStatus", th.StringType),
        th.Property("source", th.StringType),
    ).to_dict()


class FormsStream(hubspotV3Stream):
    """Forms Stream"""

    name = "forms"
    path = "marketing/v3/forms/"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("fieldGroups", th.CustomType({"type": ["array", "string"]})),
        th.Property("configuration", th.CustomType({"type": "object"})),
        th.Property("displayOptions", th.CustomType({"type": "object"})),
        th.Property("legalConsentOptions", th.CustomType({"type": "object"})),
        th.Property("formType", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("userId", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "form_id": record["id"],
        }


class FormSubmissionsStream(hubspotV1Stream):
    """FormSubmissions Stream"""

    name = "form_submissions"
    records_jsonpath = "$.results[*]"
    parent_stream_type = FormsStream
    # NOTE: There is no primary_key for this stream
    replication_key = "submittedAt"
    path = "/form-integrations/v1/submissions/forms/{form_id}"
    properties_url = "properties/v2/form_submissions/properties"

    schema = th.PropertiesList(
        th.Property("form_id", th.StringType),
        th.Property("values", th.CustomType({"type": ["array", "string"]})),
        th.Property("submittedAt", th.DateTimeType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = super().post_process(row, context)
        row["form_id"] = context.get("form_id")
        return row


class OwnersStream(hubspotV3Stream):
    """Owners Stream"""

    name = "owners"
    path = "crm/v3/owners/"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("email", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("teams", th.CustomType({"type": ["array", "string"]})),
        th.Property("archived", th.BooleanType),
        th.Property("userId", th.IntegerType),
        th.Property("userIdIncludingInactive", th.IntegerType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("archivedAt", th.DateTimeType),
        th.Property("_hg_archived", th.BooleanType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = super().post_process(row, context)
        row["_hg_archived"] = False
        return row

class ListsStream(hubspotV1Stream):
    """Lists Stream"""

    name = "lists"
    path = "contacts/v1/lists"
    records_jsonpath = "$.lists[*]"
    primary_keys = ["listId", "updatedAt"]
    replication_key = "updatedAt"
    page_size = 250

    schema = th.PropertiesList(
        th.Property("listId", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("authorId", th.IntegerType),
        th.Property("portalId", th.IntegerType),
        th.Property("internalListId", th.IntegerType),
        th.Property("dynamic", th.BooleanType),
        th.Property("listType", th.StringType),
        th.Property("metaData", th.CustomType({"type": ["object", "string"]})),
        th.Property("filters", th.CustomType({"type": ["array", "string"]})),
        th.Property("teamIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("deleteable", th.BooleanType),
        th.Property("archived", th.BooleanType),
    ).to_dict()


class DealsPipelinesStream(hubspotV1Stream):
    """Deal Pipelines Stream"""

    name = "deals_pipelines"
    path = "crm-pipelines/v1/pipelines/deals"
    records_jsonpath = "$.results[*]"
    primary_keys = ["pipelineId"]
    replication_key = None
    page_size = 250

    schema = th.PropertiesList(
        th.Property("pipelineId", th.StringType),
        th.Property("objectType", th.StringType),
        th.Property("label", th.StringType),
        th.Property("displayOrder", th.IntegerType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("active", th.BooleanType),
        th.Property("stages", th.ArrayType(th.ObjectType(
            th.Property("stageId", th.StringType),
            th.Property("label", th.StringType),
            th.Property("displayOrder", th.IntegerType),
            th.Property("active", th.BooleanType),
        ))),
    ).to_dict()

    def get_deleted_stages(self, row):
        if not "stages" in row:
            row["stages"] = []

        # get stages ids to not send dups
        row_stages = [stage["stageId"] for stage in row.get("stages")]
        # get audit history of each pipeline
        stages_history = requests.get(
            f"{self.url_base}crm/v3/pipelines/deals/{row['pipelineId']}/audit",
            headers=self.authenticator.auth_headers or {},
        )
        # join all stages from history
        stages_ = []
        for obj in stages_history.json().get("results", []):
            obj_stages = json.loads(obj.get("rawObject", "{}")).get("stages") or []
            stages_.extend(obj_stages)

        # drop stages with stageId null
        stages_ = [stage for stage in stages_ if stage.get("stageId")]

        # Sort data by stageId and by updatedAt (None values at the end)
        stages_.sort(
            key=lambda x: (
                x["stageId"],
                x["updatedAt"] if x["updatedAt"] is not None else float("-inf")  # Handle None in updatedAt
            ),
            reverse=True,
        )

        # Group by stageId and pick the first entry per group
        for stage_id, group in groupby(stages_, key=lambda x: x["stageId"]):
            # only add stages that are not in the response
            if stage_id in row_stages:
                continue
            # get stage with highest updatedAt value, if no updatedAt values it means the
            # stage was never updated after creation so we chose the one with updatedAt value null
            group = list(group)
            best = next((item for item in group if item["updatedAt"] is not None), None)
            if best is None:
                best = group[0]
            # change active value to false (to know it's a deleted stage) and add it to the row
            created_at = next(iter([stage.get("createdAt") for stage in group if stage.get("createdAt")]), None)
            row["stages"].append(
                {
                    "label": best.get("label"),
                    "displayOrder": best.get("displayOrder"),
                    "metadata": best.get("metadata"),
                    "stageId": best.get("stageId"),
                    "createdAt": created_at,
                    "updatedAt": best.get("updatedAt"),
                    "active": False,
                }
            )

        return row

    def post_process(self, row, context) -> dict:
        row = super().post_process(row, context)
        row = self.get_deleted_stages(row)
        return row


class ContactListsStream(hubspotStreamSchema):
    """Lists Stream"""

    name = "contact_list"
    parent_stream_type = None
    records_jsonpath = "$.lists[*]"
    primary_keys = ["id", "name"]
    replication_key = None
    path = "/contacts/v1/lists"

    def _request_records(self, params: dict) -> Iterable[dict]:
        """Request and return a page of records from the API."""
        try:
            records = list(super().request_records(params))
        except FatalAPIError:
            logging.info("Couldn't get schema for path: /contacts/v1/lists")
            return []

        return records

    @cached_property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        # Init request session
        self._requests_session = requests.Session()
        # Get the data from Hubspot
        try:
            records = self._request_records(dict())
        except FatalAPIError:
            self.logger.warning("Failed to run discover on dynamic stream ContactListsStream properties.")
            records = []

        properties = []
        property_names = set()
        name = "id"
        property_names.add(name)
        properties.append(th.Property(name, th.StringType))
        name = "name"
        property_names.add(name)
        properties.append(th.Property(name, th.StringType))
        # Loop through all records – some objects have different keys
        for record in records:
            # Add the new property to our list
            name = f"{record['listId']}"
            property_names.add(name)
            properties.append(th.Property(name, th.StringType))

        # Return the list as a JSON Schema dictionary object
        property_list = th.PropertiesList(*properties).to_dict()

        return property_list

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        selected_properties = self.selected_properties
        ignore = ["id", "name"]
        # Init request session
        self._requests_session = requests.Session()
        # Get the data from Hubspot
        records = list(self.request_records(dict()))
        for property in selected_properties:
            if property not in ignore:
                list_name = next(
                    r["name"] for r in records if str(r["listId"]) == property
                )
                yield {"id": property.strip(), "name": list_name}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "list_id": record["id"],
        }


class ContactListData(hubspotV1Stream):
    """Lists Stream"""

    name = "contact_list_data"
    records_jsonpath = "$.contacts[*]"
    parent_stream_type = ContactListsStream
    primary_keys = ["vid", "listId"]
    replication_key = None
    path = "/contacts/v1/lists/{list_id}/contacts/all"
    properties_url = "properties/v1/contacts/properties"

    base_properties = [
        th.Property("vid", th.IntegerType),
        th.Property("addedAt", th.DateTimeType),
        th.Property("portal-id", th.IntegerType),
        th.Property("listId", th.IntegerType),
    ]

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        super().post_process(row, context)
        row["listId"] = int(context.get("list_id"))
        return row


class ObjectSearchV3(hubspotV3SearchStream):
    """Base Object Stream"""

    primary_keys = ["id"]
    replication_key = "updatedAt"

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("_hg_archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
    ]


class ContactsV3Stream(ObjectSearchV3):
    """Contacts Stream"""

    name = "contacts_v3"
    path = "crm/v3/objects/contacts/search"
    properties_url = "properties/v1/contacts/properties"

    @property
    def replication_key(self):
        if self.config.get("filter_contacts_created_at"):
            return "createdAt"
        return "updatedAt"

    @property
    def replication_key_filter(self):
        if self.config.get("filter_contacts_created_at"):
            return "createdate"
        return "lastmodifieddate"

    def apply_catalog(self, catalog) -> None:
        self._tap_input_catalog = catalog
        catalog_entry = catalog.get_stream(self.name)
        if catalog_entry:
            self.primary_keys = catalog_entry.key_properties
            if catalog_entry.replication_method:
                self.forced_replication_method = catalog_entry.replication_method

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class ContactsHistoryPropertiesStream(hubspotHistoryV3Stream):
    """Contacts History Properties Stream"""

    name = "contacts_history_properties"
    path = "crm/v3/objects/contacts/{id}"
    properties_url = "properties/v1/contacts/properties"
    additional_prarams = {"propertiesWithHistory": True}
    parent_stream_type = ContactsV3Stream
    bulk_child = False
    records_jsonpath = "$[*]"
    primary_keys = ["id"]

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
        th.Property("propertiesWithHistory", th.CustomType({"type": ["object", "string"]})),
    ]

    def _write_schema_message(self) -> None:
        """Write out a SCHEMA message with the stream schema."""
        for schema_message in self._generate_schema_messages():
            schema_message.schema = th.PropertiesList(*self.base_properties).to_dict()
            singer.write_message(schema_message)

class ArchivedStream(hubspotV3Stream):

    def post_process(self, row, context):
        row = super().post_process(row, context)

        # add archived value to _hg_archived
        row["_hg_archived"] = True
        rep_key = self.get_starting_timestamp(context)
        if rep_key:
            rep_key = rep_key.replace(tzinfo=pytz.utc)
            archived_at = parse(row['archivedAt']).replace(tzinfo=pytz.utc)
            if archived_at > rep_key:
                return row
            return None
        return row


class CompaniesStream(ObjectSearchV3):
    """Companies Stream"""

    name = "companies"
    object_type = "companies"
    path = "crm/v3/objects/companies/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v1/companies/properties"


class FullsyncCompaniesStream(hubspotV2Stream):
    """Companies Fullsync Stream"""

    name = "fullsync_companies"
    object_type = "companies"
    path = "companies/v2/companies/paged"
    replication_key = "updatedAt"
    records_jsonpath = "$.companies[*]"
    properties_url = "properties/v2/companies/properties"
    limit = 250

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("_hg_archived", th.BooleanType),
    ]

    def post_process(self, row, context):
        row = super().post_process(row, context)
        # add archived value to _hg_archived
        row["_hg_archived"] = row["archived"]
        return row

    @cached_property
    def selected(self) -> bool:
        """Check if stream is selected.
        Returns:
            True if the stream is selected.
        """
        # It has to be in the catalog or it will cause issues
        if not self._tap.catalog.get("fullsync_companies"):
            return False

        try:
            # Make this stream auto-select if companies is selected
            self._tap.catalog["fullsync_companies"] = self._tap.catalog["companies"]
            params = self.get_url_params(dict(), None)
            if len(urlencode(params)) > 15000:
                self.logger.warn("Too many properties to use fullsync companies. Defaulting back to normal companies stream.")
                # TODO: in this case we can fall back and split the requests
                return False
            return self.mask.get((), False) or self._tap.catalog["companies"].metadata.get(()).selected
        except:
            return self.mask.get((), False)

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message.
        Args:
            record: A single stream record.
        """
        for record_message in self._generate_record_messages(record):
            # force this to think it's the companies stream
            record_message.stream = "companies"
            singer.write_message(record_message)

    @property
    def metadata(self):
        new_metadata = super().metadata
        new_metadata[("properties", "hs_lastmodifieddate")].selected = True
        new_metadata[("properties", "hs_lastmodifieddate")].selected_by_default = True
        return new_metadata


class ArchivedCompaniesStream(ArchivedStream):
    """Archived Companies Stream"""

    name = "companies_archived"
    replication_key = "archivedAt"
    path = "crm/v3/objects/companies?archived=true"
    properties_url = "properties/v1/companies/properties"
    primary_keys = ["id"]

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("_hg_archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType)
    ]

    @property
    def selected(self) -> bool:
        """Check if stream is selected.
        Returns:
            True if the stream is selected.
        """
        # It has to be in the catalog or it will cause issues
        if not self._tap.catalog.get("companies_archived"):
            return False

        try:
            # Make this stream auto-select if companies is selected
            self._tap.catalog["companies_archived"] = self._tap.catalog["companies"]
            return self.mask.get((), False) or self._tap.catalog["companies"].metadata.get(()).selected
        except:
            return self.mask.get((), False)

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message.
        Args:
            record: A single stream record.
        """
        for record_message in self._generate_record_messages(record):
            # force this to think it's the companies stream
            record_message.stream = "companies"
            singer.write_message(record_message)

    @property
    def metadata(self):
        new_metadata = super().metadata
        new_metadata[("properties", "archivedAt")].selected = True
        new_metadata[("properties", "archivedAt")].selected_by_default = True
        return new_metadata

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if len(urlencode(params)) > 3000:
            params["properties"] = "id,createdAt,updatedAt,archived,archivedAt"
        return params


class TicketsStream(ObjectSearchV3):
    """Companies Stream"""

    name = "tickets"
    path = "crm/v3/objects/tickets/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/tickets/properties"

class DealsStream(ObjectSearchV3):
    """Deals Stream"""

    name = "deals"
    path = "crm/v3/objects/deals/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v1/deals/properties"

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}
    
class DealsHistoryPropertiesStream(hubspotHistoryV3Stream):
    """Deals Stream"""

    name = "deals_history_properties"
    path = "crm/v3/objects/deals/{id}"
    properties_url = "properties/v1/deals/properties"
    additional_prarams = {"propertiesWithHistory": True}
    parent_stream_type = DealsStream
    bulk_child = False
    records_jsonpath = "$[*]"
    base_properties = [
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
        th.Property("propertiesWithHistory", th.CustomType({"type": ["object", "string"]})),
    ]


class DealsAssociationParent(hubspotV1Stream):
    name = "deals_association_parent"
    path = "deals/v1/deal/paged"
    replication_key = None
    primary_keys = ["id"]

    records_jsonpath = "$.deals[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
    ).to_dict()

    def post_process(self, row, context):
        row = super().post_process(row, context)
        row["id"] = str(row["dealId"])
        return row

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages. """
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_context_bulk = {"ids": []}
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    child_context_bulk["ids"].append(child_context)
                if len(child_context_bulk["ids"])>=self.bulk_child_size:
                    self._sync_children(child_context_bulk)
                    child_context_bulk = {"ids": []}
                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if len(child_context_bulk):
                self._sync_children(child_context_bulk)
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()


class ArchivedDealsStream(ArchivedStream):
    """Archived Deals Stream"""

    name = "deals_archived"
    replication_key = "archivedAt"
    path = "crm/v3/objects/deals?archived=true"
    properties_url = "properties/v1/deals/properties"
    primary_keys = ["id"]

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("_hg_archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("dealname", th.StringType),
        th.Property("hubspot_owner_id", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("hs_mrr", th.StringType),
        th.Property("dealstage", th.StringType),
        th.Property("pipeline", th.StringType),
        th.Property("dealtype", th.StringType),
        th.Property("hs_createdate", th.DateTimeType),
        th.Property("createdate", th.DateTimeType),
        th.Property("hs_lastmodifieddate", th.DateTimeType),
        th.Property("closedate", th.DateTimeType)
    ]

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if len(urlencode(params)) > 3000:
            params["properties"] = "id,createdAt,updatedAt,archivedAt,dealname,hubspot_owner_id,amount,hs_mrr,dealstage,pipeline,dealtype,hs_createdate,createdate,hs_lastmodifieddate,closedate,archived"
        return params

    @property
    def metadata(self):
        new_metadata = super().metadata
        new_metadata[("properties", "archivedAt")].selected = True
        new_metadata[("properties", "archivedAt")].selected_by_default = True
        return new_metadata

    @property
    def selected(self) -> bool:
        """Check if stream is selected.
        Returns:
            True if the stream is selected.
        """
        # It has to be in the catalog or it will cause issues
        if not self._tap.catalog.get("deals_archived"):
            return False

        try:
            # Make this stream auto-select if deals is selected
            self._tap.catalog["deals_archived"] = self._tap.catalog["deals"]
            return self.mask.get((), False) or self._tap.catalog["deals"].metadata.get(()).selected
        except:
            return self.mask.get((), False)

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message.
        Args:
            record: A single stream record.
        """
        for record_message in self._generate_record_messages(record):
            # force this to think it's the deals stream
            record_message.stream = "deals"
            singer.write_message(record_message)


class ProductsStream(ObjectSearchV3):
    """Products Stream"""

    name = "products"
    path = "crm/v3/objects/products/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/products/properties"


class EmailsStream(ObjectSearchV3):
    """Emails Stream"""

    name = "emails"
    path = "crm/v3/objects/emails/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/emails/properties"

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class NotesStream(ObjectSearchV3):
    """Notes Stream"""

    name = "notes"
    path = "crm/v3/objects/notes/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/notes/properties"

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class CallsStream(ObjectSearchV3):
    """Calls Stream"""

    name = "calls"
    path = "crm/v3/objects/calls/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/calls/properties"

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class TasksStream(ObjectSearchV3):
    """Tasks Stream"""

    name = "tasks"
    path = "crm/v3/objects/tasks/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/tasks/properties"

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class MeetingsStream(ObjectSearchV3):
    """Meetings Stream"""

    name = "meetings"
    path = "crm/v3/objects/meetings/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/meetings/properties"

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class LineItemsStream(ObjectSearchV3):
    """Products Stream"""

    name = "lineitems"
    path = "crm/v3/objects/line_items/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/line_items/properties"


class ArchivedLineItemsStream(ArchivedStream):
    """Line Items Stream"""

    name = "lineitems_archived"
    replication_key = "archivedAt"
    path = "crm/v3/objects/line_items?archived=true"
    properties_url = "properties/v2/line_items/properties"
    primary_keys = ["id"]

    base_properties = [
        th.Property("id", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("_hg_archived", th.BooleanType),
        th.Property("archivedAt", th.DateTimeType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType)
    ]

    @property
    def metadata(self):
        new_metadata = super().metadata
        new_metadata[("properties", "archivedAt")].selected = True
        new_metadata[("properties", "archivedAt")].selected_by_default = True
        return new_metadata

    @property
    def selected(self) -> bool:
        """Check if stream is selected.

        Returns:
            True if the stream is selected.
        """
        # It has to be in the catalog or it will cause issues
        if not self._tap.catalog.get("lineitems_archived"):
            return False

        try:
            # Make this stream auto-select if lineitems is selected
            self._tap.catalog["lineitems_archived"] = self._tap.catalog["lineitems"]
            return self.mask.get((), False) or self._tap.catalog["lineitems"].metadata.get(()).selected
        except:
            return self.mask.get((), False)

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message.

        Args:
            record: A single stream record.
        """
        for record_message in self._generate_record_messages(record):
            # force this to think it's the lineitems stream
            record_message.stream = "lineitems"
            singer.write_message(record_message)

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if len(urlencode(params)) > 3000:
            params["properties"] = "id,createdAt,updatedAt,archived,archivedAt"
        return params


class ListSearchV3Stream(hubspotV3SingleSearchStream):

    name = "lists_v3"
    primary_keys = ["id"]
    path = "crm/v3/lists/search"
    replication_key = "updatedAt"
    replication_key_filter = "hs_last_record_added_at"
    records_jsonpath = "$.lists[*]"


    @property
    def replication_key(self):
        return "updatedAt"

    schema = th.PropertiesList(
        th.Property("listId", th.NumberType()),
        th.Property("listVersion", th.NumberType()),
        th.Property("createdAt", th.DateTimeType()),
        th.Property("updatedAt", th.DateTimeType()),
        th.Property("filtersUpdateAt", th.DateTimeType()),
        th.Property("processingStatus", th.StringType()),
        th.Property("createdById", th.NumberType()),
        th.Property("updatedById", th.NumberType()),
        th.Property("processingType", th.StringType()),
        th.Property("objectTypeId", th.StringType()),
        th.Property("name", th.StringType()),
        th.Property("additionalProperties", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()

    def apply_catalog(self, catalog) -> None:
        self._tap_input_catalog = catalog
        catalog_entry = catalog.get_stream(self.name)
        if catalog_entry:
            self.primary_keys = catalog_entry.key_properties
            if catalog_entry.replication_method:
                self.forced_replication_method = catalog_entry.replication_method

    def get_child_context(self, record, context):
        return {
            "list_id": record["listId"],
        }


class ListMembershipV3Stream(hubspotV3Stream):
    """
    List members - child stream from ListsStream
    """

    name = "list_membership_v3"
    path = "crm/v3/lists/{list_id}/memberships"
    records_jsonpath = "$[*]"
    parent_stream_type = ListSearchV3Stream
    primary_keys = ["list_id"]

    schema = th.PropertiesList(
        th.Property("results", th.CustomType({"type": ["array", "string"]})),
        th.Property("list_id", th.IntegerType),
    ).to_dict()

    def post_process(self, row, context):
        row = super().post_process(row, context)
        row["list_id"] = context["list_id"]
        return row


class AssociationDealsStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = DealsAssociationParent

    schema = th.PropertiesList(
        th.Property("from_id", th.StringType),
        th.Property("to_id", th.StringType),
        th.Property("typeId", th.NumberType),
        th.Property("category", th.StringType),
        th.Property("label", th.StringType),
        th.Property("associationTypes", th.CustomType({"type": ["array", "object"]})),
    ).to_dict()

class AssociationContactsStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = ContactsV3Stream

    schema = th.PropertiesList(
        th.Property("from_id", th.StringType),
        th.Property("to_id", th.StringType),
        th.Property("typeId", th.NumberType),
        th.Property("category", th.StringType),
        th.Property("label", th.StringType),
        th.Property("associationTypes", th.CustomType({"type": ["array", "object"]})),
    ).to_dict()


class AssociationDealsCompaniesStream(AssociationDealsStream):
    """Association Deals -> Companies Stream"""

    name = "associations_deals_companies"
    path = "crm/v4/associations/deals/companies/batch/read"


class AssociationDealsContactsStream(AssociationDealsStream):
    """Association Deals -> Contacts Stream"""

    name = "associations_deals_contacts"
    path = "crm/v4/associations/deals/contacts/batch/read"


class AssociationDealsLineItemsStream(AssociationDealsStream):
    """Association Deals -> LineItems Stream"""

    name = "associations_deals_line_items"
    path = "crm/v4/associations/deals/line_items/batch/read"

class AssociationContactsTicketsStream(AssociationContactsStream):
    """Association Contacts -> Tickets Stream"""

    name = "associations_contacts_tickets"
    path = "crm/v4/associations/contacts/tickets/batch/read"

class AssociationContactsStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = ContactsV3Stream

    schema = th.PropertiesList(
        th.Property("from_id", th.StringType),
        th.Property("to_id", th.StringType),
        th.Property("typeId", th.NumberType),
        th.Property("category", th.StringType),
        th.Property("label", th.StringType),
        th.Property("associationTypes", th.CustomType({"type": ["array", "object"]})),
    ).to_dict()


class AssociationContactsCompaniesStream(AssociationContactsStream):
    """Association Contacts -> Companies Stream"""

    name = "associations_contacts_companies"
    path = "crm/v4/associations/contacts/companies/batch/read"

class MarketingEmailsStream(hubspotV1Stream):
    """Dispositions Stream"""

    name = "marketing_emails"
    path = "marketing-emails/v1/emails"
    records_jsonpath = "$.objects.[*]"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("ab", th.BooleanType),
        th.Property("abHoursToWait", th.IntegerType),
        th.Property("abSampleSizeDefault", th.CustomType({"type": ["number", "string"]})),
        th.Property("abSamplingDefault", th.CustomType({"type": ["number", "string"]})),
        th.Property("abSuccessMetric", th.CustomType({"type": ["number", "string"]})),
        th.Property("abTestPercentage", th.IntegerType),
        th.Property("abVariation", th.BooleanType),
        th.Property("absoluteUrl", th.StringType),
        th.Property("allEmailCampaignIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("abSuccessMetric", th.CustomType({"type": ["number", "string"]})),
        th.Property("analyticsPageType", th.StringType),
        th.Property("archived", th.BooleanType),
        th.Property("author", th.StringType),
        th.Property("authorAt", th.IntegerType),
        th.Property("authorEmail", th.StringType),
        th.Property("authorName", th.StringType),
        th.Property("authorUserId", th.IntegerType),
        th.Property("blogRssSettings", th.StringType),
        th.Property("campaign", th.StringType),
        th.Property("campaignName", th.StringType),
        th.Property("campaignUtm", th.StringType),
        th.Property("canSpamSettingsId", th.IntegerType),
        th.Property("categoryId", th.IntegerType),
        th.Property("contentTypeCategory", th.IntegerType),
        th.Property("createPage", th.BooleanType),
        th.Property("created", th.IntegerType),
        th.Property("createdById", th.IntegerType),
        th.Property("currentState", th.StringType),
        th.Property("currentlyPublished", th.BooleanType),
        th.Property("domain", th.StringType),
        th.Property("emailBody", th.StringType),
        th.Property("emailNote", th.StringType),
        th.Property("emailTemplateMode", th.StringType),
        th.Property("emailType", th.StringType),
        th.Property("emailbodyPlaintext", th.StringType),
        th.Property("feedbackEmailCategory", th.StringType),
        th.Property("feedbackSurveyId", th.NumberType),
        th.Property("flexAreas", th.CustomType({"type": ["object", "string"]})),
        th.Property("freezeDate", th.IntegerType),
        th.Property("fromName", th.StringType),
        th.Property("htmlTitle", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("isGraymailSuppressionEnabled", th.BooleanType),
        th.Property("isLocalTimezoneSend", th.BooleanType),
        th.Property("isPublished", th.BooleanType),
        th.Property("isRecipientFatigueSuppressionEnabled", th.BooleanType),
        th.Property("lastEditSessionId", th.IntegerType),
        th.Property("lastEditUpdateId", th.IntegerType),
        th.Property("layoutSections", th.CustomType({"type": ["object", "string"]})),
        th.Property("leadFlowId", th.CustomType({"type": ["number", "string"]})),
        th.Property("liveDomain", th.StringType),
        th.Property("mailingListsExcluded", th.CustomType({"type": ["array", "string"]})),
        th.Property("mailingListsIncluded", th.CustomType({"type": ["array", "string"]})),
        th.Property("maxRssEntries", th.IntegerType),
        th.Property("metaDescription", th.StringType),
        th.Property("name", th.StringType),
        th.Property("pageExpiryEnabled", th.BooleanType),
        th.Property("pageRedirected", th.BooleanType),
        th.Property("portalId", th.IntegerType),
        th.Property("previewKey", th.StringType),
        th.Property("processingStatus", th.StringType),
        th.Property("publishDate", th.IntegerType),
        th.Property("publishImmediately", th.BooleanType),
        th.Property("publishedUrl", th.StringType),
        th.Property("replyTo", th.StringType),
        th.Property("resolvedDomain", th.StringType),
        th.Property("rssEmailByText", th.StringType),
        th.Property("rssEmailClickThroughText", th.StringType),
        th.Property("rssEmailCommentText", th.StringType),
        th.Property("rssEmailEntryTemplateEnabled", th.BooleanType),
        th.Property("rssEmailImageMaxWidth", th.IntegerType),
        th.Property("rssEmailUrl", th.StringType),
        th.Property("scrubsSubscriptionLinks", th.BooleanType),
        th.Property("slug", th.StringType),
        th.Property("smartEmailFields", th.CustomType({"type": ["object", "string"]})),
        th.Property("state", th.StringType),
        th.Property("styleSettings", th.CustomType({"type": ["object", "string"]})),
        th.Property("subcategory", th.StringType),
        th.Property("subject", th.StringType),
        th.Property("subscription", th.NumberType),
        th.Property("subscriptionName", th.StringType),
        th.Property("teamPerms", th.CustomType({"type": ["array", "string"]})),
        th.Property("templatePath", th.StringType),
        th.Property("transactional", th.BooleanType),
        th.Property("unpublishedAt", th.IntegerType),
        th.Property("updated", th.IntegerType),
        th.Property("updatedById", th.IntegerType),
        th.Property("url", th.StringType),
        th.Property("useRssHeadlineAsSubject", th.BooleanType),
        th.Property("userPerms", th.CustomType({"type": ["array", "string"]})),
        th.Property("vidsExcluded", th.CustomType({"type": ["array", "string"]})),
        th.Property("vidsIncluded", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

class PostalMailStream(ObjectSearchV3):
    """Owners Stream"""

    name = "postal_mail"
    path = "crm/v3/objects/postal_mail/search"
    primary_keys = ["id"]
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/postal_mail/properties"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("hs_timestamp", th.DateTimeType),
        th.Property("hs_postal_mail_body", th.StringType),
        th.Property("hubspot_owner_id", th.StringType),
        th.Property("hs_attachment_ids", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("hs_createdate", th.DateTimeType),
        th.Property("hs_lastmodifieddate", th.DateTimeType),
        th.Property("hs_object_id", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("associations", th.CustomType({"type": ["object", "array"]})),
    ).to_dict()

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class CommunicationsStream(ObjectSearchV3):
    """Owners Stream"""

    name = "communications"
    path = "crm/v3/objects/communications/search"
    primary_keys = ["id"]
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/communications/properties"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("hs_communication_channel_type", th.StringType),
        th.Property("hs_communication_logged_from", th.StringType),
        th.Property("hs_communication_body", th.StringType),
        th.Property("hs_object_id", th.StringType),
        th.Property("hs_timestamp", th.DateTimeType),
        th.Property("hs_createdate", th.DateTimeType),
        th.Property("hs_lastmodifieddate", th.DateTimeType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("associations", th.CustomType({"type": ["object", "array"]})),
    ).to_dict()

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}


class QuotesStream(ObjectSearchV3):
    """Products Stream"""

    name = "quotes"
    path = "crm/v3/objects/quotes/search"
    replication_key_filter = "hs_lastmodifieddate"
    properties_url = "properties/v2/quotes/properties"

class AssociationQuotesDealsStream(AssociationDealsStream):
    """Association Quotes -> Deals Stream"""

    name = "associations_quotes_deals"
    path = "crm/v4/associations/deals/quotes/batch/read"


class CurrenciesStream(hubspotV3Stream):
    """Currencies Stream"""

    name = "currencies_exchange_rate"
    path = "settings/v3/currencies/exchange-rates"
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("createdAt", th.DateTimeType),
        th.Property("toCurrencyCode", th.StringType),
        th.Property("visibleInUI", th.BooleanType),
        th.Property("effectiveAt", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("conversionRate", th.NumberType),
        th.Property("fromCurrencyCode", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class ArchivedOwnersStream(ArchivedStream):
    """Archived Owners Stream"""

    name = "owners_archived"
    replication_key = None
    path = "crm/v3/owners/?archived=true"
    primary_keys = ["id"]

    schema = OwnersStream.schema

    @property
    def selected(self) -> bool:
        """Check if stream is selected.
        Returns:
            True if the stream is selected.
        """
        # It has to be in the catalog or it will cause issues
        if not self._tap.catalog.get("owners_archived"):
            return False
        try:
            # Make this stream auto-select if owners is selected
            self._tap.catalog["owners_archived"] = self._tap.catalog["owners"]
            return self.mask.get((), False) or self._tap.catalog["owners"].metadata.get(()).selected
        except:
            return self.mask.get((), False)

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message.
        Args:
            record: A single stream record.
        """
        for record_message in self._generate_record_messages(record):
            # force this to think it's the owners stream
            record_message.stream = "owners"
            singer.write_message(record_message)

    @property
    def metadata(self):
        new_metadata = super().metadata
        new_metadata[("properties", "archivedAt")].selected = True
        new_metadata[("properties", "archivedAt")].selected_by_default = True
        return new_metadata
    
    def post_process(self, row, context):
        # archivedAt is not in the response for static resources, using updatedAt
        row["archivedAt"] = row["updatedAt"]
        row = super().post_process(row, context)
        return row
    

# Get associations for engagements streams in v3

class AssociationMeetingsStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = MeetingsStream
    name = "associations_meetings"

    schema = association_schema


class AssociationMeetingsCompaniesStream(AssociationMeetingsStream):
    """Association Meetings -> Companies Stream"""

    name = "associations_meetings_companies"
    path = "crm/v4/associations/meetings/companies/batch/read"


class AssociationMeetingsContactsStream(AssociationMeetingsStream):
    """Association Meetings -> Contacts Stream"""

    name = "associations_meetings_contacts"
    path = "crm/v4/associations/meetings/contacts/batch/read"


class AssociationMeetingsDealsStream(AssociationMeetingsStream):
    """Association Meetings -> Deals Stream"""

    name = "associations_meetings_deals"
    path = "crm/v4/associations/meetings/deals/batch/read"


class AssociationCallsStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = CallsStream
    name = "associations_calls"

    schema = association_schema

class AssociationCallsCompaniesStream(AssociationCallsStream):
    """Association Calls -> Companies Stream"""

    name = "associations_calls_companies"
    path = "crm/v4/associations/calls/companies/batch/read"


class AssociationCallsContactsStream(AssociationCallsStream):
    """Association Calls -> Contacts Stream"""

    name = "associations_calls_contacts"
    path = "crm/v4/associations/calls/contacts/batch/read"


class AssociationCallsDealsStream(AssociationCallsStream):
    """Association Calls -> Deals Stream"""

    name = "associations_calls_deals"
    path = "crm/v4/associations/calls/deals/batch/read"


class AssociationCommunicationsStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = CommunicationsStream
    name = "associations_communications"

    schema = association_schema


class AssociationCommunicationsCompaniesStream(AssociationCommunicationsStream):
    """Association Communications -> Companies Stream"""

    name = "associations_communications_companies"
    path = "crm/v4/associations/communications/companies/batch/read"


class AssociationCommunicationsContactsStream(AssociationCommunicationsStream):
    """Association Communications -> Contacts Stream"""

    name = "associations_communications_contacts"
    path = "crm/v4/associations/communications/contacts/batch/read"


class AssociationCommunicationsDealsStream(AssociationCommunicationsStream):
    """Association Communications -> Deals Stream"""

    name = "associations_communications_deals"
    path = "crm/v4/associations/communications/deals/batch/read"


class AssociationEmailsStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = EmailsStream
    name = "associations_emails"

    schema = association_schema


class AssociationEmailsCompaniesStream(AssociationEmailsStream):
    """Association Emails -> Companies Stream"""

    name = "associations_emails_companies"
    path = "crm/v4/associations/emails/companies/batch/read"


class AssociationEmailsContactsStream(AssociationEmailsStream):
    """Association Emails -> Contacts Stream"""

    name = "associations_emails_contacts"
    path = "crm/v4/associations/emails/contacts/batch/read"


class AssociationEmailsDealsStream(AssociationEmailsStream):
    """Association Emails -> Deals Stream"""

    name = "associations_emails_deals"
    path = "crm/v4/associations/emails/deals/batch/read"


class AssociationNotesStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = NotesStream
    name = "associations_notes"

    schema = association_schema


class AssociationNotesCompaniesStream(AssociationNotesStream):
    """Association Notes -> Companies Stream"""

    name = "associations_notes_companies"
    path = "crm/v4/associations/notes/companies/batch/read"


class AssociationNotesContactsStream(AssociationNotesStream):
    """Association Notes -> Contacts Stream"""

    name = "associations_notes_contacts"
    path = "crm/v4/associations/notes/contacts/batch/read"


class AssociationNotesDealsStream(AssociationNotesStream):
    """Association Notes -> Deals Stream"""

    name = "associations_notes_deals"
    path = "crm/v4/associations/notes/deals/batch/read"


class AssociationPostalMailStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = PostalMailStream
    name = "associations_notes"

    schema = association_schema


class AssociationPostalMailCompaniesStream(AssociationPostalMailStream):
    """Association PostalMail -> Companies Stream"""

    name = "associations_postal_mail_companies"
    path = "crm/v4/associations/postal_mail/companies/batch/read"


class AssociationPostalMailContactsStream(AssociationPostalMailStream):
    """Association PostalMail -> Contacts Stream"""

    name = "associations_postal_mail_contacts"
    path = "crm/v4/associations/postal_mail/contacts/batch/read"


class AssociationPostalMailDealsStream(AssociationPostalMailStream):
    """Association PostalMail -> Deals Stream"""

    name = "associations_postal_mail_deals"
    path = "crm/v4/associations/postal_mail/deals/batch/read"


class AssociationTasksStream(hubspotV4Stream):
    """Association Base Stream"""

    primary_keys = ["from_id", "to_id"]
    parent_stream_type = TasksStream
    name = "associations_notes"

    schema = association_schema


class AssociationTasksCompaniesStream(AssociationTasksStream):
    """Association Tasks -> Companies Stream"""

    name = "associations_tasks_companies"
    path = "crm/v4/associations/tasks/companies/batch/read"


class AssociationTasksContactsStream(AssociationTasksStream):
    """Association Tasks -> Contacts Stream"""

    name = "associations_tasks_contacts"
    path = "crm/v4/associations/tasks/contacts/batch/read"


class AssociationTasksDealsStream(AssociationTasksStream):
    """Association Tasks -> Deals Stream"""

    name = "associations_tasks_deals"
    path = "crm/v4/associations/tasks/deals/batch/read"
