"""hubspot tap class."""

import os
from typing import List, Dict, Type, Any, cast
import logging
from singer_sdk.helpers._compat import final

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError

from tap_hubspot_beta.client_v3 import hubspotV3Stream, DynamicDiscoveredHubspotV3Stream
from tap_hubspot_beta.streams import (
    AccountStream,
    AssociationDealsCompaniesStream,
    AssociationDealsContactsStream,
    AssociationDealsLineItemsStream,
    AssociationContactsCompaniesStream,
    AssociationContactsTicketsStream,
    CallsStream,
    CompaniesStream,
    ContactEventsStream,
    ContactListData,
    ContactListsStream,
    ContactsStream,
    DealsStream,
    EmailEventsStream,
    EmailsStream,
    FormsStream,
    FormSubmissionsStream,
    LineItemsStream,
    ArchivedLineItemsStream,
    ListsStream,
    MeetingsStream,
    NotesStream,
    OwnersStream,
    ProductsStream,
    TasksStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    ArchivedCompaniesStream,
    ArchivedDealsStream,
    DealsAssociationParent,
    FullsyncCompaniesStream,
    CurrenciesStream,
    AssociationMeetingsCompaniesStream,
    AssociationMeetingsContactsStream,
    AssociationMeetingsDealsStream,
    AssociationCallsCompaniesStream,
    AssociationCallsContactsStream,
    AssociationCallsDealsStream,
    AssociationCommunicationsCompaniesStream,
    AssociationCommunicationsContactsStream,
    AssociationCommunicationsDealsStream,
    AssociationEmailsCompaniesStream,
    AssociationEmailsContactsStream,
    AssociationEmailsDealsStream,
    AssociationNotesCompaniesStream,
    AssociationNotesContactsStream,
    AssociationNotesDealsStream,
    AssociationPostalMailCompaniesStream,
    AssociationPostalMailContactsStream,
    AssociationPostalMailDealsStream,
    AssociationTasksCompaniesStream,
    AssociationTasksContactsStream,
    AssociationTasksDealsStream,
    DealsHistoryPropertiesStream,
    ContactsHistoryPropertiesStream,
    ArchivedOwnersStream,
    ArchivedProductsStream,
    FullsyncDealsStream,
    SessionAnalyticsDailyReportsStream,
    SessionAnalyticsWeeklyReportsStream,
    SessionAnalyticsMonthlyReportsStream,
    SessionAnalyticsTotalReportStream,
    BreakdownsAnalyticsReportsGeolocationMonthlyStream,
    BreakdownsAnalyticsReportsGeolocationTotalStream,
    BreakdownsAnalyticsReportsSourcesMonthlyStream,
    BreakdownsAnalyticsReportsSourcesTotalStream,
    BreakdownsAnalyticsReportsUtmCampaignsMonthlyStream,
    BreakdownsAnalyticsReportsUtmCampaignsTotalStream,
    FormsSummaryMonthlyStream,
    TeamsStream,
    MeetingsAssociationStream,
    CallsAssociationStream,
    CommunicationsAssociationStream,
    EmailsAssociationStream,
    NotesAssociationStream,
    PostalAssociationStream,
    TasksAssociationStream,
    FormsAllStream,
    SourcesSummaryMonthlyStream,
    PagesSummaryMonthlyStream,
    LandingPagesSummaryMonthlyStream,
    LandingPagesStream,
    UtmCampaignSummaryMonthlyStream,
    GeolocationSummaryMonthlyStream,
    LeadsStream,
    FullsyncContactsV3Stream,
    DiscoverCustomObjectsStream,
    ArchivedContactsStream

)

 #When a new stream is added to the tap, it would break existing test suites.
# By allowing caller to ignore the stream we are able ensure existing tests continue to pass.
# 1. Get the environment variable IGNORE_STREAMS and split by commas
ignore_streams = os.environ.get('IGNORE_STREAMS', '').split(',')
logging.info(f"IGNORE_STREAMS: "+ os.environ.get('IGNORE_STREAMS', ''))

# 2. Get the environment variable INCLUDE_STREAMS and split by commas
include_streams = os.environ.get('INCLUDE_STREAMS', "").split(',') if os.environ.get('INCLUDE_STREAMS', "") else []
logging.info(f"INCLUDE_STREAMS: "+ os.environ.get('INCLUDE_STREAMS', ''))

# Function to add streams to STREAM_TYPES if INCLUDE_STREAMS is set, otherwise add streams to STREAM_TYPES if not in IGNORE_STREAMS
def add_streams(stream_classes):
    stream_types = []
    for stream_class in stream_classes:
        if include_streams:
            if stream_class.__name__ not in include_streams:
                logging.info(f"Ignored stream {stream_class.__name__} as it's not in INCLUDE_STREAMS.")
                continue
            stream_types.append(stream_class)
        else:
            if stream_class.__name__ not in ignore_streams:
                stream_types.append(stream_class)
            else:
                logging.info(f"Ignored stream {stream_class.__name__} as it's in IGNORE_STREAMS.")
    return stream_types

STREAM_TYPES = add_streams([
    ContactsStream,
    ListsStream,
    CompaniesStream,
    DealsStream,
    OwnersStream,
    ContactListsStream,
    ContactListData,
    ProductsStream,
    LineItemsStream,
    ArchivedLineItemsStream,
    AssociationDealsContactsStream,
    AssociationDealsCompaniesStream,
    AssociationDealsLineItemsStream,
    AssociationContactsCompaniesStream,
    AssociationContactsTicketsStream,
    AccountStream,
    FormsStream,
    FormSubmissionsStream,
    ContactEventsStream,
    EmailEventsStream,
    EmailsStream,
    NotesStream,
    MeetingsStream,
    TasksStream,
    CallsStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    ArchivedCompaniesStream,
    ArchivedDealsStream,
    DealsAssociationParent,
    FullsyncCompaniesStream,
    CurrenciesStream,
    AssociationMeetingsCompaniesStream,
    AssociationMeetingsContactsStream,
    AssociationMeetingsDealsStream,
    AssociationCallsCompaniesStream,
    AssociationCallsContactsStream,
    AssociationCallsDealsStream,
    AssociationCommunicationsCompaniesStream,
    AssociationCommunicationsContactsStream,
    AssociationCommunicationsDealsStream,
    AssociationEmailsCompaniesStream,
    AssociationEmailsContactsStream,
    AssociationEmailsDealsStream,
    AssociationNotesCompaniesStream,
    AssociationNotesContactsStream,
    AssociationNotesDealsStream,
    AssociationPostalMailCompaniesStream,
    AssociationPostalMailContactsStream,
    AssociationPostalMailDealsStream,
    AssociationTasksCompaniesStream,
    AssociationTasksContactsStream,
    AssociationTasksDealsStream,
    DealsHistoryPropertiesStream,
    ContactsHistoryPropertiesStream,
    ArchivedOwnersStream,
    ArchivedProductsStream,
    FullsyncDealsStream,
    SessionAnalyticsDailyReportsStream,
    SessionAnalyticsWeeklyReportsStream,
    SessionAnalyticsMonthlyReportsStream,
    SessionAnalyticsTotalReportStream,
    BreakdownsAnalyticsReportsGeolocationMonthlyStream,
    BreakdownsAnalyticsReportsGeolocationTotalStream,
    BreakdownsAnalyticsReportsSourcesMonthlyStream,
    BreakdownsAnalyticsReportsSourcesTotalStream,
    BreakdownsAnalyticsReportsUtmCampaignsMonthlyStream,
    BreakdownsAnalyticsReportsUtmCampaignsTotalStream,
    FormsSummaryMonthlyStream,
    TeamsStream,
    MeetingsAssociationStream,
    CallsAssociationStream,
    CommunicationsAssociationStream,
    EmailsAssociationStream,
    NotesAssociationStream,
    PostalAssociationStream,
    TasksAssociationStream,
    FormsAllStream,
    SourcesSummaryMonthlyStream,
    PagesSummaryMonthlyStream,
    LandingPagesSummaryMonthlyStream,
    LandingPagesStream,
    UtmCampaignSummaryMonthlyStream,
    GeolocationSummaryMonthlyStream,
    LeadsStream,
    FullsyncContactsV3Stream,
    DiscoverCustomObjectsStream,
    ArchivedContactsStream
])


class Taphubspot(Tap):
    """hubspot tap class."""

    name = "tap-hubspot"
    legacy_streams_mapping = {}

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("redirect_uri", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("expires_in", th.IntegerType),
        th.Property("start_date", th.DateTimeType),
        th.Property("access_token", th.StringType),
        th.Property("enable_list_selection", th.BooleanType, default=False),
        th.Property("use_legacy_streams", th.BooleanType, default=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        stream_types = list(STREAM_TYPES)

        # If enable_list_selection is false, remove ContactListsStream and ContactListData from the list
        if not self.config.get("enable_list_selection"):
            stream_types = [
                st for st in stream_types
                if st.__name__ != "ContactListsStream" and st.__name__ != "ContactListData"
            ]
        
        streams = [stream_class(tap=self) for stream_class in stream_types]

        # flag only used for testing purposes
        if self.config.get("discover_custom_objects", True):
            try:
                discover_stream = DiscoverCustomObjectsStream(tap=self)
                for record in discover_stream.get_records(context={}):
                    stream_class = self.generate_stream_class(record)
                    streams.append(stream_class(tap=self))
            except FatalAPIError as exc:
                self.logger.info(f"failed to discover custom objects. Error={exc}")

        return streams
    
    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        # add visible field to metadata
        original_catalog = self._singer_catalog
        # for some reason to_dict() gets rid of the value visible so we'll add it back before json dumping the catalog
        _catalog = self._singer_catalog.to_dict()
        new_catalog = []
        for _, stream in enumerate(_catalog["streams"]):
            # add original name
            # stream["original_name"] = original_catalog[stream["tap_stream_id"]].original_name
            stream["metadata"][-1]["metadata"]["visible"] = original_catalog[stream["tap_stream_id"]].metadata[()].visible
            new_catalog.append(stream)
        catalog =  cast(dict, {"streams": new_catalog})

        # add metadata fields to catalog
        streams = self.streams
        for stream in catalog["streams"]:
            stream_class = streams[stream["tap_stream_id"]]
            stream["stream_meta"] = {}
            if hasattr(stream_class, "load_fields_metadata") and stream["stream"] in ["deals", "lineitems", "contacts", "companies"]:
                stream_class.load_fields_metadata()
                for field in stream["schema"]["properties"]:
                    stream["schema"]["properties"][field]["field_meta"] = stream_class.fields_metadata.get(field, {})
        return catalog
    
    @property
    def streams(self) -> Dict[str, Stream]:
        """Get streams discovered or catalogued for this tap.
        Results will be cached after first execution.
        Returns:
            A mapping of names to streams, using discovery or a provided catalog.
        """
        input_catalog = self.input_catalog

        if self._streams is None:
            self._streams = {}
            for stream in self.load_streams():
                if input_catalog is not None:
                    stream.apply_catalog(input_catalog)

                # add metadata visible value
                setattr(stream.metadata[()], "visible", stream.visible_in_catalog)
                self._streams[stream.name] = stream
        return self._streams


    def generate_stream_class(self, custom_object: Dict[str, Any]) -> hubspotV3Stream:
        # check for required fields to construct the custom objects class
        required_fields = ["id", "name", "objectTypeId", "properties"]
        errors = []
        for field in required_fields:
            if not custom_object.get(field):
                errors.append(f"Missing {field} in custom object.")
        if errors:
            errors.append(f"Failed custom_object={custom_object}.")
            error_msg = "\n".join(errors)
            raise ValueError(error_msg)

        name = custom_object.get("name")
        object_type_id = custom_object.get("objectTypeId")
        properties = custom_object.get("properties")
        
        if custom_object.get("archived", False):
            name = "archived_" + name
        class_name = name + "_Stream"
        class_name = "".join(word.capitalize() for word in class_name.split("_"))

        self.logger.info(f"Creating class {class_name}")

        return type(
            class_name,
            (DynamicDiscoveredHubspotV3Stream,),
            {
                "name": name,
                "path": f"crm/v3/objects/{object_type_id}/",
                "records_jsonpath": "$.results[*]",
                "primary_keys": ["id"],
                "replication_key": "updatedAt",
                "page_size": 100,
                "schema": self.generate_schema(properties),
                "is_custom_stream": True,
            },
        )
    
    def generate_schema(self, properties: List[Dict[str, Any]]) -> dict:
        properties_list = [
            th.Property("id", th.StringType),
            th.Property("updatedAt", th.DateTimeType), 
            th.Property("createdAt", th.DateTimeType), 
            th.Property("archived", th.BooleanType)
        ]
        main_properties = [p.name for p in properties_list]

        for property in properties:
            field_name = property.get("name")
            if field_name in main_properties:
                self.logger.info(f"Skipping field, it is a default field and already included.")
                continue
            if not field_name:
                self.logger.info(f"Skipping field without name.")
                continue
            th_type = hubspotV3Stream.extract_type(property)
            properties_list.append(th.Property(field_name, th_type))
        return th.PropertiesList(*properties_list).to_dict()
    
    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type:
                parents = streams_by_type[stream_type.parent_stream_type]

                # assign parent stream dynamically, if stream has parent attribute
                if hasattr(stream_type, "parent"):
                    new_parent = [streams_by_type[stream][0] for stream in streams_by_type if stream.name == streams[0].parent]
                    if new_parent and new_parent != parents:
                        parents = new_parent

                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )

        # clean streams dict and use stream name as keys
        streams_by_type = {v[0].name:v[0] for v in streams_by_type.values()}

        # change name for v3 streams and hide streams with the same base name (personalized request HGI-5253)
        del_streams = []
        if not self.config.get("use_legacy_streams"):
            for stream in streams_by_type.values():
                self.logger.info(f"Stream name: {stream.name}")
                base_name = stream.name
                streams_by_type[base_name].original_name = base_name
                if stream.name.startswith("fullsync_"):
                    streams_by_type[base_name].visible_in_catalog = False
                    continue
                # rename v3 streams that are visible in the catalog
                if stream.name.endswith("_v3"):
                    # if stream name has v3 in it remove 'v3' from the stream name 
                    base_name = stream.name.split("_v3")[0]
                    # excluding streams with the same base name from the catalog
                    if base_name in streams_by_type.keys():
                        # if same name stream is a parent stream change name and change visible = False
                        if streams_by_type[base_name].child_streams:
                            streams_by_type[base_name].name = f"is_parent_stream_{base_name}"
                            streams_by_type[base_name].visible_in_catalog = False
                        else:
                            del_streams.append(base_name)
                    # add original name to the mapping
                    self.legacy_streams_mapping[stream.name] = base_name
                    # change v3 name to base_name
                    stream.name = base_name

        if not self.config.get("use_list_selection", False):
            del_streams = del_streams + ["contact_list", "contact_list_data"]

        # delete streams that have same name as v3 stream
        streams_by_type = {key: value for key, value in streams_by_type.items() if key not in del_streams}

        streams = streams_by_type.values()
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )


if __name__ == "__main__":
    Taphubspot.cli()
